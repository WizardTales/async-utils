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

/**
 * This is a direct acyclic graph with the special note that
 * non existing references are automatically fullfilled and do not result
 * in an error.
 *
 * This is useful to represent complex dependencies between different tasks.
 * Additionally we allow to group together instructions to ensure they are only
 * executed serially, without the explicit need to build a complete dag chain.
 *
 * This class is optimized to be used with the WorkerPool class.
 */
class DAG {
  constructor () {
    this._root = {};
    this._dep = {};
    this._new = [];
    this._groups = {};
    this._rgroups = {};
  }

  add (key, value, dep = null, group = null) {
    const newDeps = new Map();
    // sometimes we want to serialize on a common key
    if (group && this._groups[group]) {
      if (!dep) {
        dep = [this._groups[group]];
      } else {
        dep.push(this._groups[group]);
      }

      // we need to remove any before entry
      delete this._rgroups[this._groups[group]][group];
    }

    // the last added element will always lead the group
    if (group) {
      this._groups[group] = key;
      this._rgroups[key] = this._rgroups[key] || {};
      this._rgroups[key][group] = 1;
    }

    if (dep === null) {
      this._root[key] = { value, s: false, exec: [] };
      this._new.push(this._root[key]);
    } else {
      for (const x of dep) {
        if (this._root[x]) {
          this._root[x].exec.push(key);
          newDeps.set(x, 1);
        } else if (this._dep[x]) {
          this._dep[x].exec.push(key);
          newDeps.set(x, 1);
        }

        // while we can have different hirarchies of dependencies
        // we still expect them to be strictly ordered
        // this means if we can't find a dependency, it probably
        // is already done
      }

      if (newDeps.size !== 0) {
        this._dep[key] = {
          value,
          s: false,
          exec: [],
          dep: newDeps
        };
      } else {
        this._root[key] = { value, s: false, exec: [] };
        this._new.push(this._root[key]);
      }
    }
  }

  remove (key) {
    if (this._root[key]) {
      delete this._root[key];
    }

    if (this._dep[key]) {
      delete this._dep[key];
    }
  }

  finish (key) {
    if (this._root[key]) {
      for (const e of this._root[key].exec) {
        const dp = this._dep[e];
        dp.dep.delete(key);

        if (dp.dep.size === 0) {
          this._root[e] = this._dep[e];
          this._new.push(this._dep[e]);

          delete this._dep[e];
        }
      }

      if (this._rgroups[key]) {
        for (const g in this._rgroups[key]) {
          delete this._groups[g];
        }

        delete this._rgroups[key];
      }

      delete this._root[key];
    }
  }

  /**
   * Get newly inserted to the root element and make the new list
   * empty again.
   */
  getPending () {
    const p = this._new;
    this._new = [];

    return p;
  }
};

/**
 * This is a passive worker queue, it works by specifying a max
 * amount of concurrent working slots. Whenever work is added it
 * will be easer immediately put for execution, or if all slots are full
 * entered into a queue.
 *
 * As soon as one task finishes, it can either automatically execute the 
 * finish function given during the constructor, or from outside of the class.
 *
 * When a tasks finishes, it will decrease the number of jobs in work again and
 * immediately check if any work is in the backpressure queue and remove it
 * from there. Additionally it will check the getWork routine 
 * from where it can get fresh tasks from outside. The getWork routine is very
 * lazy, as it is only called after a task is finished. The quickest way to
 * fill up is from outside directly when new work is available. I.e. in 
 * combination with the DAG and the fill() method. The system can work in a
 * self looping manner.
 *
 * When no work is left and nothing added it will go idle. Whenever a new task
 * is getting added, it will start automatically from there.
 */
class WorkerPool {
  #wsize = 0;

  constructor (size, getWork, finish) {
    this.size = 0;
    this._max = size;
    this._worker = [];
    this._getWork = getWork;
    this._finish = finish;

    this._queue = [];
  }

  async _work (w) {
    ++this.size;
    ++this.#wsize;
    let working = true
    for (; working === true;) {
      await w.w().catch(() => {});
      if (typeof this._finish === 'function') {
        await this._finish(w.id);
      }

      // if still work in queue continue, otherwise add free slot position
      // back to the marker array
      if (this._queue.length !== 0) {
        w = this._queue.shift();
      } else {
        --this.#wsize;

        
        
        working = false;
      }

      // if a refill function is specified, now is the time to call it to
      // get new work and push it in
      if (typeof this._getWork === 'function') {
        const work = await this._getWork();
        for (const nw of work) {
          this.add(nw);
        }
      }
    }

    --this.size;
  }

  isRunning () {
    return this.#wsize !== 0;
  }

  async fill () {
    if (typeof this._getWork === 'function') {
      const work = await this._getWork();
      for (const nw of work) {
        this.add(nw);
      }

      return true;
    } else {
      throw new Error('fill does not work without _getWork defined.');
    }
  }

  add (w) {
    if (this.#wsize < this._max) {
      this._work(w);
    } else {
      this._queue.push(w);
    }
  }
};

// const dt = new DAG();

// dt.add('test', 'bla');
// dt.add('rvtest', 'bla', ['notexisting', 'nono']);
// dt.add('test2', 'bla');
// dt.add('test3', 'bla', ['test']);
// dt.add('test4', 'bla', ['test3', 'test2']);
//
// console.log(dt._dep, dt._root);
//
// dt.finish('test2');
// dt.finish('test');
// dt.finish('test3');
//
// console.log(dt._dep, dt._root, dt._new);
//
// console.log(dt.getPending(), dt._new);

// const sleep = (x) => new Promise((resolve) => setTimeout(resolve, x * 1000));
// const l = new WorkerPool(5, () => dt.getPending().map(x => x.value));
// dt.add('test', () => sleep(1.1).then(() => dt.finish('test')));
// dt.add('test3', () => sleep(1.2).then(() => dt.finish('test3')), ['test']);
// dt.add('test4', () => sleep(1.3).then(() => dt.finish('test4')), ['test3']);
// dt.add('test5', () => sleep(1.3).then(() => dt.finish('test5')), ['test3', 'test4']);
// dt.add('test6', () => sleep(1.3).then(() => dt.finish('test6')), ['nono']);
// dt.add('test2', () => sleep(2.3).then(() => dt.finish('test2')));
//
// l.add(() => sleep(1.1));
// setInterval(() => console.log(l), 100);
// console.log(l);

module.exports = { groupedConcurrency, DAG, WorkerPool };
