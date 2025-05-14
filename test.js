import asyncUtils from '@wizardtales/async-utils';
const { groupedConcurrency, DAG, WorkerPool } = asyncUtils;

const dag = new DAG();
const wp = new WorkerPool(5, () => dag.getPending().map(x => x.value), (x) => {
  return dag.finish(x);
});

const ins = [
  { id: 1, process: 1, processId: 1, w: async () => console.log(1, Number(process.argv[2]) ? dag : '') },
  { id: 2, depProcess: 1, depId: 1, w: async () => console.log(2, Number(process.argv[2]) ? dag : '') },
  { id: 3, process: 1, processId: 1, w: async () => console.log(3, Number(process.argv[2]) ? dag : '') },
  { id: 4, process: 2, processId: 1, w: async () => console.log(4, Number(process.argv[2]) ? dag : '') },
  { id: 5, process: 2, processId: 1, w: async () => console.log(5, Number(process.argv[2]) ? dag : '') },
  // this will be stil serialized, but we have an additional dependency outside
  // of the serialized group
  { id: 6, process: 1, processId: 1, dag: [4], w: async () => console.log(6, Number(process.argv[2]) ? dag : '') },
  { id: 7, depProcess: 1, depId: 1, w: async () => console.log(7, Number(process.argv[2]) ? dag : '') },
  { id: 8, process: 2, processId: 1, w: async () => console.log(8, Number(process.argv[2]) ? dag : '') },
  { id: 9, process: 2, processId: 1, w: async () => console.log(9, Number(process.argv[2]) ? dag : '') }
];

for (const x of ins) {
  // we have quite some strict serial processes, so we always make sure
  // to have a group created even with dag entry existing, so we can
  // guarantee to serialize, only dag entries can skip the serialization
  // or make it even stricter
  const groupKey = x.depId
    ? `${x.depProcess},${x.depId}`
    : `${x.process},${x.processId}`;

  console.log('add with gKey', groupKey, x.id);

  if (Array.isArray(x.dag)) {
    dag.add(x.id, x, x.dag, groupKey);
  } else {
    dag.add(x.id, x, null, groupKey);
  }
}

await wp.fill();
if (Number(process.argv[3])) {
  setInterval(() => console.log(dag), 100);
}
