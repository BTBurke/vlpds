// Allowlists the fields of Docker's container list and inspect responses
// that Alloy's discovery.docker and loki.source.docker read, so Env, Cmd,
// Mounts and the rest of a container's config never leave the proxy.

function pick(obj, keys) {
    var out = {};
    if (obj === null || typeof obj !== 'object') {
        return out;
    }
    keys.forEach(function (k) {
        if (obj[k] !== undefined) {
            out[k] = obj[k];
        }
    });
    return out;
}

var SUMMARY_KEYS = ['Id', 'Names', 'Image', 'ImageID', 'Created', 'Labels',
    'State', 'Status', 'Ports', 'HostConfig', 'NetworkSettings'];

// State.Health is left out: its Log holds healthcheck output.
var STATE_KEYS = ['Status', 'Running', 'Paused', 'Restarting', 'OOMKilled',
    'Dead', 'Pid', 'ExitCode', 'Error', 'StartedAt', 'FinishedAt'];

function summary(c) {
    var out = pick(c, SUMMARY_KEYS);
    out.HostConfig = pick(c.HostConfig, ['NetworkMode']);
    return out;
}

function inspect(c) {
    var out = pick(c, ['Id', 'Created', 'Name', 'Image', 'RestartCount']);
    out.State = pick(c.State, STATE_KEYS);
    // loki.source.docker dereferences Config; Tty picks the stream framing.
    out.Config = pick(c.Config, ['Tty', 'Image']);
    return out;
}

function relay(r, shape) {
    r.subrequest('/_docker' + r.uri, { args: r.variables.args || '' })
        .then(function (res) {
            r.headersOut['Content-Type'] = 'application/json';
            if (res.status !== 200) {
                // Docker's errors are {"message": ...}; the status matters:
                // a 404 is how a tailer learns its container is gone.
                r.return(res.status, res.responseText);
                return;
            }
            var body = JSON.parse(res.responseText);
            r.return(200, JSON.stringify(Array.isArray(body) ? body.map(shape) : shape(body)));
        })
        .catch(function (e) {
            r.error('docker-proxy: ' + e);
            r.return(502);
        });
}

function containerList(r) {
    relay(r, summary);
}

function containerInspect(r) {
    relay(r, inspect);
}

export default { containerList, containerInspect };
