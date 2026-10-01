package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
)

// Failure kinds. Kept as short stable strings so the per-interval breakdown is greppable.
const (
	failDecode          = "decode"            // frame could not be decoded
	failSeqGap          = "seq_gap"           // seq skipped ahead (missing events)
	failSeqReorder      = "seq_reorder"       // seq went backwards or repeated
	failCAR             = "car"               // blocks CAR unreadable, bad root, bad block hash, bad commit object
	failCommitCID       = "commit_cid"        // msg.commit != CAR root
	failDIDMismatch     = "did_mismatch"      // event DID != commit object DID
	failRevMismatch     = "rev_mismatch"      // event rev != commit object rev
	failTooBig          = "too_big"           // tooBig set (forbidden in sync 1.1)
	failRebase          = "rebase"            // rebase set (forbidden in sync 1.1)
	failOpMissingPrev   = "op_missing_prev"   // update/delete op without prev CID
	failMissingPrevData = "missing_prevdata"  // non-genesis commit without prevData
	failPrevDataInvert  = "prevdata_mismatch" // MST inversion root != prevData
	failCommitVerify    = "commit_verify"     // any other VerifyCommitMessage error
	failKeyFetch        = "key_fetch"         // could not get the account's signing key
	failSignature       = "signature"         // commit signature invalid
	failChainSince      = "chain_since"       // since != previous rev
	failChainPrevData   = "chain_prevdata"    // prevData != previous data CID
	failChainRev        = "chain_rev"         // rev not strictly greater than previous rev
	failUpstreamError   = "upstream_error"    // server sent an error frame
)

type failure struct {
	Kind   string
	Seq    int64
	DID    string
	Reason string
}

// KeySource resolves an account's current atproto signing key.
type KeySource interface {
	Get(ctx context.Context, did string) (atcrypto.PublicKey, error)
	Invalidate(did string)
}

// pdsKeySource fetches keys from the PDS's describeRepo endpoint, because the
// PDS under test mints non-resolvable did:plc identifiers.
type pdsKeySource struct {
	host   string
	client *http.Client

	mu    sync.Mutex
	cache map[string]atcrypto.PublicKey
}

func newPDSKeySource(host string) *pdsKeySource {
	return &pdsKeySource{
		host:   strings.TrimRight(host, "/"),
		client: &http.Client{Timeout: 10 * time.Second},
		cache:  map[string]atcrypto.PublicKey{},
	}
}

func (s *pdsKeySource) Invalidate(did string) {
	s.mu.Lock()
	delete(s.cache, did)
	s.mu.Unlock()
}

func (s *pdsKeySource) Get(ctx context.Context, did string) (atcrypto.PublicKey, error) {
	s.mu.Lock()
	k, ok := s.cache[did]
	s.mu.Unlock()
	if ok {
		return k, nil
	}
	u := s.host + "/xrpc/com.atproto.repo.describeRepo?repo=" + url.QueryEscape(did)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("describeRepo: HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	var out struct {
		DidDoc struct {
			VerificationMethod []struct {
				ID                 string `json:"id"`
				PublicKeyMultibase string `json:"publicKeyMultibase"`
			} `json:"verificationMethod"`
		} `json:"didDoc"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("describeRepo: bad JSON: %w", err)
	}
	vms := out.DidDoc.VerificationMethod
	if len(vms) == 0 {
		return nil, fmt.Errorf("describeRepo: didDoc has no verificationMethod")
	}
	mb := vms[0].PublicKeyMultibase
	for _, vm := range vms {
		if strings.HasSuffix(vm.ID, "#atproto") {
			mb = vm.PublicKeyMultibase
			break
		}
	}
	k, err = atcrypto.ParsePublicMultibase(mb)
	if err != nil {
		return nil, fmt.Errorf("describeRepo: publicKeyMultibase %q: %w", mb, err)
	}
	s.mu.Lock()
	s.cache[did] = k
	s.mu.Unlock()
	return k, nil
}

type chainState struct {
	Rev  string
	Data cid.Cid
}

// verifier holds per-DID chain state. Each worker owns one, and events are
// partitioned by DID, so a verifier is only ever used from one goroutine.
type verifier struct {
	keys  KeySource // nil disables signature checks
	state map[string]chainState
}

func newVerifier(keys KeySource) *verifier {
	return &verifier{keys: keys, state: map[string]chainState{}}
}

// inspectCAR reads a firehose blocks CAR, checks that every block's CID
// matches its bytes, and decodes the root block as a commit object.
func inspectCAR(b []byte) (cid.Cid, *repo.Commit, error) {
	cr, err := car.NewCarReader(bytes.NewReader(b))
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("reading CAR header: %w", err)
	}
	if cr.Header.Version != 1 {
		return cid.Undef, nil, fmt.Errorf("CAR version %d, want 1", cr.Header.Version)
	}
	if len(cr.Header.Roots) < 1 {
		return cid.Undef, nil, fmt.Errorf("CAR has no root")
	}
	root := cr.Header.Roots[0]
	var commitRaw []byte
	for {
		blk, err := cr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return root, nil, fmt.Errorf("reading CAR block: %w", err)
		}
		computed, err := blk.Cid().Prefix().Sum(blk.RawData())
		if err != nil {
			return root, nil, fmt.Errorf("hashing block %s: %w", blk.Cid(), err)
		}
		if !computed.Equals(blk.Cid()) {
			return root, nil, fmt.Errorf("block %s hashes to %s", blk.Cid(), computed)
		}
		if blk.Cid().Equals(root) {
			commitRaw = blk.RawData()
		}
	}
	if commitRaw == nil {
		return root, nil, fmt.Errorf("root block %s not in CAR", root)
	}
	var c repo.Commit
	if err := c.UnmarshalCBOR(bytes.NewReader(commitRaw)); err != nil {
		return root, nil, fmt.Errorf("decoding commit object: %w", err)
	}
	if err := c.VerifyStructure(); err != nil {
		return root, nil, fmt.Errorf("commit object: %w", err)
	}
	return root, &c, nil
}

func (v *verifier) checkSignature(ctx context.Context, did string, c *repo.Commit) *failure {
	if v.keys == nil {
		return nil
	}
	key, err := v.keys.Get(ctx, did)
	if err != nil {
		return &failure{Kind: failKeyFetch, Reason: err.Error()}
	}
	if err := c.VerifySignature(key); err == nil {
		return nil
	}
	// The key may have rotated since we cached it; refetch once.
	v.keys.Invalidate(did)
	key, err = v.keys.Get(ctx, did)
	if err != nil {
		return &failure{Kind: failKeyFetch, Reason: "after signature miss: " + err.Error()}
	}
	if err := c.VerifySignature(key); err != nil {
		return &failure{Kind: failSignature, Reason: fmt.Sprintf("key %s: %v", key.Multibase(), err)}
	}
	return nil
}

// verifyCommit runs every sync 1.1 check on a #commit and advances the DID's
// chain state. Returns all failures found (empty means the commit verified).
func (v *verifier) verifyCommit(ctx context.Context, msg *comatproto.SyncSubscribeRepos_Commit) []failure {
	var fails []failure
	add := func(kind, format string, args ...any) {
		fails = append(fails, failure{Kind: kind, Seq: msg.Seq, DID: msg.Repo, Reason: fmt.Sprintf(format, args...)})
	}
	did := msg.Repo

	if msg.TooBig {
		add(failTooBig, "tooBig flag set")
	}
	if msg.Rebase {
		add(failRebase, "rebase flag set")
	}

	root, commit, err := inspectCAR(msg.Blocks)
	if err != nil {
		add(failCAR, "%v", err)
		// Without the commit object we can't know the new data CID; forget the
		// chain so the next event re-seeds instead of cascading failures.
		delete(v.state, did)
		return fails
	}
	if !root.Equals(cid.Cid(msg.Commit)) {
		add(failCommitCID, "msg.commit=%s but CAR root=%s", cid.Cid(msg.Commit), root)
	}
	if commit.DID != did {
		add(failDIDMismatch, "event repo=%s commit.did=%s", did, commit.DID)
	}
	if commit.Rev != msg.Rev {
		add(failRevMismatch, "event rev=%s commit.rev=%s", msg.Rev, commit.Rev)
	}

	// indigo's VerifyCommitMessage silently returns success (skipping the
	// inversion) when any update/delete op lacks prev, so check it ourselves.
	for _, op := range msg.Ops {
		if (op.Action == "update" || op.Action == "delete") && op.Prev == nil {
			add(failOpMissingPrev, "%s op on %s has no prev", op.Action, op.Path)
		}
	}
	// indigo also skips the inversion check (only logging) when prevData is
	// null. That is only legitimate for a repo's genesis commit (since=null).
	if msg.PrevData == nil && msg.Since != nil {
		add(failMissingPrevData, "since=%s but prevData is null", *msg.Since)
	}

	if _, err := repo.VerifyCommitMessage(ctx, msg); err != nil {
		if strings.Contains(err.Error(), "prevData") {
			add(failPrevDataInvert, "%v (prevData=%s)", err, fmtLink(msg.PrevData))
		} else {
			add(failCommitVerify, "%v", err)
		}
	}

	if f := v.checkSignature(ctx, did, commit); f != nil {
		add(f.Kind, "%s", f.Reason)
	}

	if prev, ok := v.state[did]; ok {
		if msg.Since == nil {
			add(failChainSince, "since is null but previous rev is %s", prev.Rev)
		} else if *msg.Since != prev.Rev {
			add(failChainSince, "since=%s, previous rev=%s", *msg.Since, prev.Rev)
		}
		if msg.PrevData != nil && !cid.Cid(*msg.PrevData).Equals(prev.Data) {
			add(failChainPrevData, "prevData=%s, previous data=%s", cid.Cid(*msg.PrevData), prev.Data)
		}
		if !revGreater(msg.Rev, prev.Rev) {
			add(failChainRev, "rev=%s not greater than previous rev=%s", msg.Rev, prev.Rev)
		}
	}
	v.state[did] = chainState{Rev: commit.Rev, Data: commit.Data}
	return fails
}

// verifySync checks a #sync event's commit and resets the DID's chain state.
func (v *verifier) verifySync(ctx context.Context, msg *comatproto.SyncSubscribeRepos_Sync) []failure {
	var fails []failure
	add := func(kind, format string, args ...any) {
		fails = append(fails, failure{Kind: kind, Seq: msg.Seq, DID: msg.Did, Reason: fmt.Sprintf(format, args...)})
	}
	did := msg.Did
	if _, err := syntax.ParseDatetime(msg.Time); err != nil {
		add(failCAR, "#sync time: %v", err)
	}
	_, commit, err := inspectCAR(msg.Blocks)
	if err != nil {
		add(failCAR, "#sync: %v", err)
		delete(v.state, did)
		return fails
	}
	if commit.DID != did {
		add(failDIDMismatch, "#sync did=%s commit.did=%s", did, commit.DID)
	}
	if commit.Rev != msg.Rev {
		add(failRevMismatch, "#sync rev=%s commit.rev=%s", msg.Rev, commit.Rev)
	}
	if f := v.checkSignature(ctx, did, commit); f != nil {
		add(f.Kind, "#sync: %s", f.Reason)
	}
	if prev, ok := v.state[did]; ok && revLess(commit.Rev, prev.Rev) {
		add(failChainRev, "#sync rev=%s is older than previous rev=%s", commit.Rev, prev.Rev)
	}
	v.state[did] = chainState{Rev: commit.Rev, Data: commit.Data}
	return fails
}

// TIDs are fixed-length base32-sortable strings, so string order is time order.
func revGreater(a, b string) bool { return a > b }
func revLess(a, b string) bool    { return a < b }

func fmtLink(l *lexutil.LexLink) string {
	if l == nil {
		return "null"
	}
	return cid.Cid(*l).String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
