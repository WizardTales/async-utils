async function groupedConcurrency(iterator, promise, opts) {
  const state = {};
  const f = iterator.reduce((o, x) => {
    if (!o[x[opts.key]]) {
      o[x[opts.key]] = [x];
      state[x[opts.key]] = { a: false, i: 0 };
    } else o[x[opts.key]].push(x);

    return o;
  }, {});

  const keys = Object.keys(f);
  const len = keys.length - 1;
  const mod = len + 1;
  const promises = [];
  const result = {};
  let i = -1;

  for (let c = 0; c < opts.concurrency; ++c) {
    promises[c] = (async () => {
      for (;;) {
        let m = 0;
        // len + 1 to hit itself once
        while (
          (state[keys[++i % mod]].i >= f[keys[i % mod]].length ||
            state[keys[i % mod]].a) &&
          m++ < len + 1
        );

        if (m >= len) return Promise.resolve();

        const v = keys[i % mod];
        const u = f[v];
        state[v].a = true;

        let p = promise(u[state[v].i]).catch((e) => {
          return Promise.resolve({ err: e });
        });
        ++state[v].i;

        // delayed to avoid double execution due to to late increment of
        // ___it_g
        p = await p;
        if (!result[v]) result[v] = [p];
        else result[v].push(p);

        state[v].a = false;
      }
    })();
  }

  await Promise.all(promises);
  return result;
}

module.exports = { groupedConcurrency };
