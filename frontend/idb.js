// minimal outbox for offline queuing
const DB_NAME = 'kikoba_local';
const DB_VER = 1;
let db;
function openDb(){
  return new Promise((res,rej)=>{
    const req = indexedDB.open(DB_NAME, DB_VER);
    req.onupgradeneeded = e => {
      const d = e.target.result;
      if(!d.objectStoreNames.contains('outbox')) d.createObjectStore('outbox', {keyPath:'id', autoIncrement:true});
    };
    req.onsuccess = ()=>{ db=req.result; res(db); };
    req.onerror = ()=> rej(req.error);
  });
}
async function addOutbox(item){
  if(!db) await openDb();
  return new Promise((res,rej)=>{
    const tx = db.transaction('outbox','readwrite');
    const s = tx.objectStore('outbox');
    const r = s.add(item);
    r.onsuccess = ()=> res(r.result);
    r.onerror = ()=> rej(r.error);
  });
}
async function getOutboxAll(){
  if(!db) await openDb();
  return new Promise((res,rej)=>{
    const tx = db.transaction('outbox','readonly');
    const s = tx.objectStore('outbox');
    const r = s.getAll();
    r.onsuccess = ()=> res(r.result);
    r.onerror = ()=> rej(r.error);
  });
}
async function clearOutbox(){
  if(!db) await openDb();
  return new Promise((res,rej)=>{
    const tx = db.transaction('outbox','readwrite');
    const s = tx.objectStore('outbox');
    const r = s.clear();
    r.onsuccess = ()=> res();
    r.onerror = ()=> rej(r.error);
  });
}
export { openDb, addOutbox, getOutboxAll, clearOutbox };
