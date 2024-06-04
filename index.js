async function groupedConcurrency (iterator, promise, opts) {
  const state = {};
  const f = iterator.reduce((o, x) => {
    if (!o[x[opts.key]]) {
      o[x[opts.key]] = [x];
      state[x[opts.key]] = { a: false, i: 0 };
    } else o[x[opts.key]].push(x);

    return o;
  }, {});

  const keys = Object.keys(f);
  const len = keys.length;
  const mod = len;
  const promises = [];
  const result = {};
  const rounds = opts.delay ? 2 : 1;
  let i = -1;

  for (let c = 0; c < opts.concurrency; ++c) {
    promises[c] = (async () => {
      for (;;) {
        let m = 0;
        // len + 1 to hit itself once
        // assumes about equal sizing of all group elements
        // faster by avoiding manipulating an array structure by using pop
        // could be improved by manipulating on the index
        while (
          (state[keys[++i % mod]].i >= f[keys[i % mod]].length * rounds ||
            state[keys[i % mod]].a) &&
          m++ < len
        );

        if (m >= len) return Promise.resolve();

        // when we allow a promise to be delayed (put back into queue) we
        // reset the actual value of i to its corresponding value for another
        // scan through round
        if (opts.delay && state[keys[i % mod]].i >= f[keys[i % mod]].length) {
          state[keys[i % mod]].i %= f[keys[i % mod]].length;
        }

        const v = keys[i % mod];
        const u = f[v];
        state[v].a = true;

        let p = promise(u[state[v].i]).catch(e => {
          return Promise.resolve({ err: e });
        });
        ++state[v].i;

        // delayed to avoid double execution due to to late increment of
        // ___it_g
        p = await p;

        if (opts.delay && p?.__delay === true) {
          state[v].a = false;
          continue;
        }

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
