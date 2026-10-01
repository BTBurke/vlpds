package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/repo/mst"
	"github.com/bluesky-social/indigo/atproto/syntax"
	lexutil "github.com/bluesky-social/indigo/lex/util"

	"github.com/gorilla/websocket"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"github.com/multiformats/go-multihash"
)

func TestMain(m *testing.M) {
	// Silence indigo's per-event Info/Warn logging, as main does.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	os.Exit(m.Run())
}

func kinds(fs []failure) []string {
	out := []string{}
	for _, f := range fs {
		out = append(out, f.Kind)
	}
	sort.Strings(out)
	return out
}

func wantKinds(t *testing.T, fs []failure, want ...string) {
	t.Helper()
	got := kinds(fs)
	sort.Strings(want)
	if want == nil {
		want = []string{}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("failure kinds = %v, want %v\nfull: %+v", got, want, fs)
	}
}

// TestIndigoFixtures runs indigo's real-network firehose fixtures through the
// per-event commit path (no signature check: keys are not in the fixtures).
// They predate sync 1.1 (no prevData) and three come from bridgyfed with MST
// slices too thin to invert; indigo's own sync_test.go has those three
// commented out for the same reason. What matters here is that the CAR, CID,
// DID and rev checks accept real production data, and that the sync 1.1
// rules flag exactly what they should.
func TestIndigoFixtures(t *testing.T) {
	want := map[string][]string{
		// since=null: genesis-style, so missing prevData is allowed; inversion fails on partial MST.
		"firehose_commit_4621317030.json": {failCommitVerify},
		"firehose_commit_4621317332.json": {failCommitVerify},
		// since set but no prevData: a sync 1.1 violation, plus partial-MST inversion failure.
		"firehose_commit_4621332152.json": {failMissingPrevData, failCommitVerify},
		// inverts fine; only the missing prevData is flagged.
		"firehose_commit_4623075231.json": {failMissingPrevData},
	}
	files, err := filepath.Glob("testdata/firehose_commit_*.json")
	if err != nil || len(files) != len(want) {
		t.Fatalf("expected %d fixtures, got %v (%v)", len(want), files, err)
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			var msg comatproto.SyncSubscribeRepos_Commit
			if err := json.Unmarshal(b, &msg); err != nil {
				t.Fatal(err)
			}
			v := newVerifier(nil)
			fails := v.verifyCommit(context.Background(), &msg)
			wantKinds(t, fails, want[filepath.Base(f)]...)
			if st, ok := v.state[msg.Repo]; !ok || st.Rev != msg.Rev {
				t.Fatalf("chain state not seeded: %+v", st)
			}

			// Same message, but tamper one byte inside a block: hash check must catch it.
			bad := msg
			bad.Blocks = append([]byte(nil), msg.Blocks...)
			bad.Blocks[len(bad.Blocks)-1] ^= 0xff
			wantKinds(t, newVerifier(nil).verifyCommit(context.Background(), &bad), failCAR)
		})
	}
}

// ---- synthetic signed repo -------------------------------------------------

// collectBS satisfies blockstore.Blockstore for mst.WriteDiffBlocks, which only calls Put.
type collectBS struct {
	blockstore.Blockstore
	blks map[cid.Cid]blocks.Block
}

func (c *collectBS) Put(_ context.Context, b blocks.Block) error {
	c.blks[b.Cid()] = b
	return nil
}

type fakeRepo struct {
	t       *testing.T
	did     string
	key     *atcrypto.PrivateKeyK256
	clock   *syntax.TIDClock
	records map[string]cid.Cid
	rev     string
	data    *cid.Cid
	seq     int64
	nextRev string // if set, used (once) instead of the clock
}

func newFakeRepo(t *testing.T, did string) *fakeRepo {
	k, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		t.Fatal(err)
	}
	return &fakeRepo{t: t, did: did, key: k, clock: syntax.NewTIDClock(0), records: map[string]cid.Cid{}}
}

func cborText(s string) []byte {
	if len(s) > 255 {
		panic("too long")
	}
	if len(s) < 24 {
		return append([]byte{0x60 | byte(len(s))}, s...)
	}
	return append([]byte{0x78, byte(len(s))}, s...)
}

func dagCID(b []byte) cid.Cid {
	c, err := cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256).Sum(b)
	if err != nil {
		panic(err)
	}
	return c
}

type opSpec struct {
	action string // create | update | delete
	path   string
}

// commit applies ops and returns a fully valid sync 1.1 #commit. The CAR carries
// the whole MST (not a minimal slice), which is fine for exercising the checker.
func (r *fakeRepo) commit(ops ...opSpec) *comatproto.SyncSubscribeRepos_Commit {
	recBlocks := map[cid.Cid][]byte{}
	var repoOps []*comatproto.SyncSubscribeRepos_RepoOp
	for i, op := range ops {
		rop := &comatproto.SyncSubscribeRepos_RepoOp{Action: op.action, Path: op.path}
		if prev, ok := r.records[op.path]; ok {
			l := lexutil.LexLink(prev)
			rop.Prev = &l
		}
		switch op.action {
		case "create", "update":
			data := cborText(fmt.Sprintf("%s#%d#%d", op.path[len(op.path)-4:], r.seq, i))
			c := dagCID(data)
			recBlocks[c] = data
			r.records[op.path] = c
			l := lexutil.LexLink(c)
			rop.Cid = &l
		case "delete":
			delete(r.records, op.path)
		}
		repoOps = append(repoOps, rop)
	}
	return r.finish(repoOps, recBlocks)
}

func (r *fakeRepo) finish(repoOps []*comatproto.SyncSubscribeRepos_RepoOp, recBlocks map[cid.Cid][]byte) *comatproto.SyncSubscribeRepos_Commit {
	t := r.t
	ctx := context.Background()
	tree, err := mst.LoadTreeFromMap(r.records)
	if err != nil {
		t.Fatal(err)
	}
	bs := &collectBS{blks: map[cid.Cid]blocks.Block{}}
	root, err := tree.WriteDiffBlocks(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}
	rev := r.clock.Next().String()
	if r.nextRev != "" {
		rev, r.nextRev = r.nextRev, ""
	}
	commit, commitCID, commitBytes := r.signedCommit(*root, rev, r.key)

	var buf bytes.Buffer
	if err := car.WriteHeader(&car.CarHeader{Roots: []cid.Cid{commitCID}, Version: 1}, &buf); err != nil {
		t.Fatal(err)
	}
	put := func(c cid.Cid, b []byte) {
		if err := carutil.LdWrite(&buf, c.Bytes(), b); err != nil {
			t.Fatal(err)
		}
	}
	put(commitCID, commitBytes)
	for c, b := range bs.blks {
		put(c, b.RawData())
	}
	for c, b := range recBlocks {
		put(c, b)
	}

	r.seq++
	msg := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   r.did,
		Rev:    commit.Rev,
		Seq:    r.seq,
		Time:   syntax.DatetimeNow().String(),
		Commit: lexutil.LexLink(commitCID),
		Blocks: buf.Bytes(),
		Ops:    repoOps,
		Blobs:  []lexutil.LexLink{},
	}
	if r.rev != "" {
		since := r.rev
		msg.Since = &since
	}
	if r.data != nil {
		l := lexutil.LexLink(*r.data)
		msg.PrevData = &l
	}
	r.rev, r.data = commit.Rev, &commit.Data
	return msg
}

func (r *fakeRepo) signedCommit(data cid.Cid, rev string, key atcrypto.PrivateKey) (*repo.Commit, cid.Cid, []byte) {
	c := &repo.Commit{DID: r.did, Version: repo.ATPROTO_REPO_VERSION, Data: data, Rev: rev}
	if err := c.Sign(key); err != nil {
		r.t.Fatal(err)
	}
	var b bytes.Buffer
	if err := c.MarshalCBOR(&b); err != nil {
		r.t.Fatal(err)
	}
	return c, dagCID(b.Bytes()), b.Bytes()
}

// syncEvent emits a #sync for the repo's current records (a fresh commit, as
// after an out-of-band repo import), and resets the chain to it.
func (r *fakeRepo) syncEvent() *comatproto.SyncSubscribeRepos_Sync {
	tree, err := mst.LoadTreeFromMap(r.records)
	if err != nil {
		r.t.Fatal(err)
	}
	root, err := tree.RootCID()
	if err != nil {
		r.t.Fatal(err)
	}
	commit, commitCID, commitBytes := r.signedCommit(*root, r.clock.Next().String(), r.key)
	var buf bytes.Buffer
	_ = car.WriteHeader(&car.CarHeader{Roots: []cid.Cid{commitCID}, Version: 1}, &buf)
	_ = carutil.LdWrite(&buf, commitCID.Bytes(), commitBytes)
	r.seq++
	r.rev, r.data = commit.Rev, &commit.Data
	return &comatproto.SyncSubscribeRepos_Sync{Did: r.did, Rev: commit.Rev, Seq: r.seq, Time: syntax.DatetimeNow().String(), Blocks: buf.Bytes()}
}

// describeRepoServer serves the didDoc shape the checker reads keys from.
func describeRepoServer(t *testing.T, repos ...*fakeRepo) *httptest.Server {
	byDID := map[string]*fakeRepo{}
	for _, r := range repos {
		byDID[r.did] = r
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/xrpc/com.atproto.repo.describeRepo" {
			http.NotFound(w, req)
			return
		}
		r, ok := byDID[req.URL.Query().Get("repo")]
		if !ok {
			http.Error(w, `{"error":"RepoNotFound"}`, http.StatusBadRequest)
			return
		}
		pub, _ := r.key.PublicKey()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"did": r.did,
			"didDoc": map[string]any{
				"id": r.did,
				"verificationMethod": []map[string]any{{
					"id": r.did + "#atproto", "type": "Multikey", "controller": r.did,
					"publicKeyMultibase": pub.Multibase(),
				}},
			},
		})
	}))
}

func paths(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("app.bsky.feed.post/3l%011d", i)
	}
	return out
}

func TestSyntheticChainClean(t *testing.T) {
	r := newFakeRepo(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	srv := describeRepoServer(t, r)
	defer srv.Close()
	v := newVerifier(newPDSKeySource(srv.URL))
	ctx := context.Background()
	p := paths(40)

	// genesis: since=null, prevData=null
	wantKinds(t, v.verifyCommit(ctx, r.commit(opSpec{"create", "app.bsky.actor.profile/self"})))
	for i := 0; i < 30; i++ {
		wantKinds(t, v.verifyCommit(ctx, r.commit(opSpec{"create", p[i]})))
	}
	// multi-op commit mixing all three actions
	wantKinds(t, v.verifyCommit(ctx, r.commit(
		opSpec{"update", p[3]}, opSpec{"delete", p[7]}, opSpec{"create", p[35]}, opSpec{"delete", p[20]},
	)))
	// empty commit: prevData must equal the unchanged data CID
	wantKinds(t, v.verifyCommit(ctx, r.commit()))
	// #sync resets chain; the next commit chains off the sync's rev/data
	wantKinds(t, v.verifySync(ctx, r.syncEvent()))
	wantKinds(t, v.verifyCommit(ctx, r.commit(opSpec{"delete", p[0]})))
}

func TestSyntheticViolations(t *testing.T) {
	ctx := context.Background()
	p := paths(10)

	// setup builds a repo with a few commits already verified, returning the
	// verifier and repo so a subtest can tamper with the next event.
	setup := func(t *testing.T) (*verifier, *fakeRepo) {
		r := newFakeRepo(t, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")
		srv := describeRepoServer(t, r)
		t.Cleanup(srv.Close)
		v := newVerifier(newPDSKeySource(srv.URL))
		for i := 0; i < 5; i++ {
			wantKinds(t, v.verifyCommit(ctx, r.commit(opSpec{"create", p[i]})))
		}
		return v, r
	}

	t.Run("wrong since", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"create", p[6]})
		s := "2222222222222"
		msg.Since = &s
		wantKinds(t, v.verifyCommit(ctx, msg), failChainSince)
	})
	t.Run("null since mid-chain", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"create", p[6]})
		msg.Since = nil
		wantKinds(t, v.verifyCommit(ctx, msg), failChainSince)
	})
	t.Run("prevData not matching inversion or chain", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"create", p[6]})
		l := lexutil.LexLink(dagCID(cborText("bogus")))
		msg.PrevData = &l
		wantKinds(t, v.verifyCommit(ctx, msg), failPrevDataInvert, failChainPrevData)
	})
	t.Run("missing prevData", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"create", p[6]})
		msg.PrevData = nil
		wantKinds(t, v.verifyCommit(ctx, msg), failMissingPrevData)
	})
	t.Run("ops omit a change", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"create", p[6]}, opSpec{"create", p[7]})
		msg.Ops = msg.Ops[:1]
		wantKinds(t, v.verifyCommit(ctx, msg), failPrevDataInvert)
	})
	t.Run("delete without prev", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"delete", p[2]})
		msg.Ops[0].Prev = nil
		// indigo's VerifyCommitMessage returns success early for this "legacy" op; only our check fires.
		wantKinds(t, v.verifyCommit(ctx, msg), failOpMissingPrev)
	})
	t.Run("signed by wrong key", func(t *testing.T) {
		v, r := setup(t)
		good := r.key
		r.key, _ = atcrypto.GeneratePrivateKeyK256()
		msg := r.commit(opSpec{"create", p[6]})
		r.key = good // describeRepo still serves the real key
		wantKinds(t, v.verifyCommit(ctx, msg), failSignature)
	})
	t.Run("key rotation is picked up", func(t *testing.T) {
		v, r := setup(t)
		r.key, _ = atcrypto.GeneratePrivateKeyK256() // server now serves the new key; cache is stale
		wantKinds(t, v.verifyCommit(ctx, r.commit(opSpec{"create", p[6]})))
	})
	t.Run("commit cid mismatch", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"create", p[6]})
		msg.Commit = lexutil.LexLink(dagCID(cborText("x")))
		wantKinds(t, v.verifyCommit(ctx, msg), failCommitCID)
	})
	t.Run("rev does not advance", func(t *testing.T) {
		v, r := setup(t)
		prevRev := r.rev
		r.nextRev = syntax.NewTIDFromTime(time.Now().Add(-time.Hour), 0).String()
		msg := r.commit(opSpec{"create", p[6]})
		if msg.Rev >= prevRev {
			t.Fatalf("test setup: rev %s not below %s", msg.Rev, prevRev)
		}
		wantKinds(t, v.verifyCommit(ctx, msg), failChainRev)
	})
	t.Run("event rev differs from commit rev", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"create", p[6]})
		msg.Rev = syntax.NewTIDNow(0).String()
		// indigo rejects this too (as commit_verify); chain_rev is fine since it's newer.
		wantKinds(t, v.verifyCommit(ctx, msg), failRevMismatch, failCommitVerify)
	})
	t.Run("tooBig and rebase", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"create", p[6]})
		msg.TooBig, msg.Rebase = true, true
		wantKinds(t, v.verifyCommit(ctx, msg), failTooBig, failRebase)
	})
	t.Run("mid-stream start accepts first event", func(t *testing.T) {
		_, r := setup(t)
		srv := describeRepoServer(t, r)
		defer srv.Close()
		fresh := newVerifier(newPDSKeySource(srv.URL))
		wantKinds(t, fresh.verifyCommit(ctx, r.commit(opSpec{"create", p[6]})))
		wantKinds(t, fresh.verifyCommit(ctx, r.commit(opSpec{"create", p[7]})))
	})
	t.Run("sync with wrong key", func(t *testing.T) {
		v, r := setup(t)
		good := r.key
		r.key, _ = atcrypto.GeneratePrivateKeyK256()
		ev := r.syncEvent()
		r.key = good
		wantKinds(t, v.verifySync(ctx, ev), failSignature)
	})
	t.Run("unknown repo key fetch", func(t *testing.T) {
		v, _ := setup(t)
		other := newFakeRepo(t, "did:plc:cccccccccccccccccccccccc")
		wantKinds(t, v.verifyCommit(ctx, other.commit(opSpec{"create", p[0]})), failKeyFetch)
	})
}

func TestSeqChecker(t *testing.T) {
	sc := newSeqChecker()
	var got []string
	for _, s := range []int64{10, 11, 12, 15, 16, 14, 16, 17} {
		if f := sc.check(s); f != nil {
			got = append(got, fmt.Sprintf("%s@%d", f.Kind, s))
		}
	}
	want := "seq_gap@15,seq_reorder@14,seq_reorder@16"
	if strings.Join(got, ",") != want {
		t.Fatalf("got %v, want %s", got, want)
	}
	if sc.first != 10 || sc.last != 17 {
		t.Fatalf("first/last = %d/%d", sc.first, sc.last)
	}
}

func encodeFrame(t *testing.T, typ string, body interface{ MarshalCBOR(io.Writer) error }) []byte {
	var b bytes.Buffer
	// canonical key order: "t" before "op"
	b.Write([]byte{0xa2})
	b.Write(cborText("t"))
	b.Write(cborText(typ))
	b.Write(cborText("op"))
	b.WriteByte(0x01)
	if err := body.MarshalCBOR(&b); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// TestStreamEndToEnd serves frames over a real websocket and runs the same
// read loop and worker pool main uses.
func TestStreamEndToEnd(t *testing.T) {
	r1 := newFakeRepo(t, "did:plc:dddddddddddddddddddddddd")
	r2 := newFakeRepo(t, "did:plc:eeeeeeeeeeeeeeeeeeeeeeee")
	keySrv := describeRepoServer(t, r1, r2)
	defer keySrv.Close()

	var frames [][]byte
	seq := int64(100)
	emit := func(typ string, body interface{ MarshalCBOR(io.Writer) error }, setSeq func(int64)) {
		seq++
		setSeq(seq)
		frames = append(frames, encodeFrame(t, typ, body))
	}
	p := paths(20)
	for i := 0; i < 20; i++ {
		r := r1
		if i%2 == 1 {
			r = r2
		}
		c := r.commit(opSpec{"create", p[i]})
		emit("#commit", c, func(s int64) { c.Seq = s })
		if i == 9 {
			seq += 3 // gap
		}
	}
	id := &comatproto.SyncSubscribeRepos_Identity{Did: r1.did, Time: syntax.DatetimeNow().String()}
	emit("#identity", id, func(s int64) { id.Seq = s })
	acct := &comatproto.SyncSubscribeRepos_Account{Did: r2.did, Active: true, Time: syntax.DatetimeNow().String()}
	emit("#account", acct, func(s int64) { acct.Seq = s })
	sy := r1.syncEvent()
	emit("#sync", sy, func(s int64) { sy.Seq = s })
	c := r1.commit(opSpec{"delete", p[0]})
	emit("#commit", c, func(s int64) { c.Seq = s })

	up := websocket.Upgrader{}
	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/xrpc/com.atproto.sync.subscribeRepos" || req.URL.Query().Get("cursor") != "100" {
			http.Error(w, "bad request "+req.URL.String(), 400)
			return
		}
		conn, err := up.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for _, f := range frames {
			_ = conn.WriteMessage(websocket.BinaryMessage, f)
		}
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"))
		time.Sleep(100 * time.Millisecond)
	}))
	defer wsSrv.Close()

	u, err := subscribeURL(wsSrv.URL, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var out bytes.Buffer
	st := newStats(&out)
	pool := startWorkers(4, newPDSKeySource(keySrv.URL), st)
	sc := newSeqChecker()
	reason := readLoop(context.Background(), conn, config{}, st, sc, pool)
	pool.close()
	st.summary(sc.last, sc.first, reason)
	t.Log(out.String())

	if !strings.Contains(reason, "closed by server") {
		t.Fatalf("reason = %q", reason)
	}
	if st.events.Load() != 24 || st.commits.Load() != 21 || st.commitsOK.Load() != 21 ||
		st.syncsOK.Load() != 1 || st.identity.Load() != 1 || st.account.Load() != 1 {
		t.Fatalf("unexpected counters:\n%s", out.String())
	}
	if st.failures() != 1 || st.fails[failSeqGap] != 1 {
		t.Fatalf("want exactly one seq_gap failure, got %v", st.fails)
	}
	if !strings.Contains(out.String(), "seq jumped 110 -> 114 (3 missing)") {
		t.Fatalf("gap detail missing:\n%s", out.String())
	}
}

func TestCheckCanonical(t *testing.T) {
	link := append([]byte{0xd8, 0x2a, 0x58, 0x25, 0x00}, dagCID([]byte{0xf6}).Bytes()...)
	ok := [][]byte{
		{0xf6},
		{0xa2, 0x61, 'a', 0x01, 0x62, 'a', 'a', 0x02}, // {"a":1,"aa":2}: shorter key first
		{0x18, 0x18}, // 24
		append([]byte{0xa1, 0x61, 'l'}, link...),
	}
	for _, b := range ok {
		if err := checkCanonical(b); err != nil {
			t.Errorf("%x: %v", b, err)
		}
	}
	bad := map[string][]byte{
		"unsorted keys":      {0xa2, 0x62, 'a', 'a', 0x01, 0x61, 'b', 0x02},
		"duplicate keys":     {0xa2, 0x61, 'a', 0x01, 0x61, 'a', 0x02},
		"non-minimal int":    {0x18, 0x01},
		"non-minimal length": {0x78, 0x01, 'a'},
		"indefinite array":   {0x9f, 0x01, 0xff},
		"float":              {0xfb, 0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18},
		"half float":         {0xf9, 0x3c, 0x00},
		"undefined":          {0xf7},
		"tag 1":              {0xc1, 0x01},
		"trailing bytes":     {0x01, 0x02},
		"int map key":        {0xa1, 0x01, 0x02},
	}
	for name, b := range bad {
		if err := checkCanonical(b); err == nil {
			t.Errorf("%s (%x) accepted", name, b)
		}
	}
}

func TestEventChecks(t *testing.T) {
	ctx := context.Background()
	p := paths(10)
	setup := func(t *testing.T) (*verifier, *fakeRepo) {
		r := newFakeRepo(t, "did:plc:ffffffffffffffffffffffff")
		srv := describeRepoServer(t, r)
		t.Cleanup(srv.Close)
		v := newVerifier(newPDSKeySource(srv.URL))
		wantKinds(t, v.verifyCommit(ctx, r.commit(opSpec{"create", p[0]})))
		return v, r
	}

	t.Run("non-canonical record block", func(t *testing.T) {
		v, r := setup(t)
		// {"b":1,"a":2}: unsorted keys, otherwise a fine record
		rec := []byte{0xa2, 0x61, 'b', 0x01, 0x61, 'a', 0x02}
		c := dagCID(rec)
		r.records[p[1]] = c
		l := lexutil.LexLink(c)
		ops := []*comatproto.SyncSubscribeRepos_RepoOp{{Action: "create", Path: p[1], Cid: &l}}
		wantKinds(t, v.verifyCommit(ctx, r.finish(ops, map[cid.Cid][]byte{c: rec})), failNonCanonical)
	})
	t.Run("float in record block", func(t *testing.T) {
		v, r := setup(t)
		rec := []byte{0xa1, 0x61, 'a', 0xfb, 0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18}
		c := dagCID(rec)
		r.records[p[1]] = c
		l := lexutil.LexLink(c)
		ops := []*comatproto.SyncSubscribeRepos_RepoOp{{Action: "create", Path: p[1], Cid: &l}}
		wantKinds(t, v.verifyCommit(ctx, r.finish(ops, map[cid.Cid][]byte{c: rec})), failNonCanonical)
	})
	t.Run("duplicate op path", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"create", p[1]})
		dup := *msg.Ops[0]
		msg.Ops = append(msg.Ops, &dup)
		// indigo rejects the op list too
		wantKinds(t, v.verifyCommit(ctx, msg), failDupOpPath, failCommitVerify)
	})
	t.Run("blocks too big", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"create", p[1]})
		// trailing garbage past the CAR is unreadable too
		msg.Blocks = append(msg.Blocks, make([]byte, maxCommitBlocks)...)
		fails := v.verifyCommit(ctx, msg)
		if k := kinds(fails); len(k) == 0 || !strings.Contains(strings.Join(k, ","), failBlocksSize) {
			t.Fatalf("got %v", fails)
		}
	})
	t.Run("time goes backwards", func(t *testing.T) {
		v, r := setup(t)
		msg := r.commit(opSpec{"create", p[1]})
		msg.Time = syntax.DatetimeNow().Time().Add(-time.Hour).Format(syntax.AtprotoDatetimeLayout)
		wantKinds(t, v.verifyCommit(ctx, msg), failTimeOrder)
		// the next event in order is fine; one with a bad time is not
		wantKinds(t, v.verifyCommit(ctx, r.commit(opSpec{"create", p[2]})))
		msg = r.commit(opSpec{"create", p[3]})
		msg.Time = "yesterday"
		wantKinds(t, v.verifyCommit(ctx, msg), failBadField, failCommitVerify)
		id := &comatproto.SyncSubscribeRepos_Identity{Did: r.did, Time: "2001-01-01T00:00:00Z"}
		wantKinds(t, v.verifyIdentity(id), failTimeOrder)
	})
	t.Run("identity fields", func(t *testing.T) {
		v := newVerifier(nil)
		now := syntax.DatetimeNow().String()
		good, bad := "alice.example.com", "not a handle"
		wantKinds(t, v.verifyIdentity(&comatproto.SyncSubscribeRepos_Identity{Did: "did:plc:aaaa", Handle: &good, Time: now}))
		wantKinds(t, v.verifyIdentity(&comatproto.SyncSubscribeRepos_Identity{Did: "did:plc:aaaa", Time: now}))
		wantKinds(t, v.verifyIdentity(&comatproto.SyncSubscribeRepos_Identity{Did: "did:plc:aaaa", Handle: &bad, Time: now}), failBadField)
		wantKinds(t, v.verifyIdentity(&comatproto.SyncSubscribeRepos_Identity{Did: "alice", Time: now}), failBadField)
	})
	t.Run("account fields", func(t *testing.T) {
		v := newVerifier(nil)
		now := syntax.DatetimeNow().String()
		st := func(s string) *string { return &s }
		wantKinds(t, v.verifyAccount(&comatproto.SyncSubscribeRepos_Account{Did: "did:plc:aaaa", Active: true, Time: now}))
		wantKinds(t, v.verifyAccount(&comatproto.SyncSubscribeRepos_Account{Did: "did:plc:aaaa", Status: st("takendown"), Time: now}))
		wantKinds(t, v.verifyAccount(&comatproto.SyncSubscribeRepos_Account{Did: "did:plc:aaaa", Active: true, Status: st("deactivated"), Time: now}), failBadField)
		wantKinds(t, v.verifyAccount(&comatproto.SyncSubscribeRepos_Account{Did: "did:plc:aaaa", Status: st("sleepy"), Time: now}), failBadField)
		wantKinds(t, v.verifyAccount(&comatproto.SyncSubscribeRepos_Account{Did: "did:plc:aaaa", Active: true, Time: "x"}), failBadField)
	})
	t.Run("non-canonical frame body", func(t *testing.T) {
		id := &comatproto.SyncSubscribeRepos_Identity{Did: "did:plc:aaaa", Seq: 1, Time: syntax.DatetimeNow().String()}
		f := encodeFrame(t, "#identity", id)
		ev, err := decodeFrame(f)
		if err != nil || ev.NonCanonical != "" {
			t.Fatalf("canonical frame: %v %q", err, ev.NonCanonical)
		}
		// the same fields with "seq" before "did": decodes the same, isn't canonical
		var b bytes.Buffer
		b.Write(f[:len(f)-len(frameBody(t, id))])
		b.WriteByte(0xa3)
		b.Write(cborText("seq"))
		b.WriteByte(0x01)
		b.Write(cborText("did"))
		b.Write(cborText(id.Did))
		b.Write(cborText("time"))
		b.Write(cborText(id.Time))
		ev, err = decodeFrame(b.Bytes())
		if err != nil || ev.Identity.Seq != 1 || !strings.Contains(ev.NonCanonical, "frame body") {
			t.Fatalf("non-canonical body: %v %q", err, ev.NonCanonical)
		}
	})
}

func frameBody(t *testing.T, body interface{ MarshalCBOR(io.Writer) error }) []byte {
	var b bytes.Buffer
	if err := body.MarshalCBOR(&b); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
