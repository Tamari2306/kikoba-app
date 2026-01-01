// main.js - client interactions (uses Fetch to backend)
import { openDb, addOutbox, getOutboxAll, clearOutbox } from './idb.js';
const API = 'http://127.0.0.1:5000/api';

async function init(){
  await openDb();
  bindForms();
  fetchMembers();
  fetchLoans();
  window.addEventListener('online', trySync);
  trySync();
}

function bindForms(){
  document.getElementById('member-form').onsubmit = async e=>{
    e.preventDefault();
    const fd = new FormData(e.target);
    const payload = { name: fd.get('name'), phone: fd.get('phone') };
    try {
      const res = await fetch(`${API}/members/`, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
      if(res.ok) { alert('Member added'); fetchMembers(); e.target.reset(); }
    } catch(err) {
      await addOutbox({ type: 'member_create', payload }); alert('Offline — queued');
    }
  };

  document.getElementById('contrib-form').onsubmit = async e=>{
    e.preventDefault();
    const fd = new FormData(e.target);
    const payload = { member_id: fd.get('member_id'), type: fd.get('type'), amount: fd.get('amount'), date: fd.get('date') };
    try {
      const res = await fetch(`${API}/contributions/`, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
      if(res.ok) { alert('Contribution added'); e.target.reset(); }
    } catch(err) {
      await addOutbox({ type: 'contrib_create', payload }); alert('Offline — queued');
    }
  };

  document.getElementById('loan-form').onsubmit = async e=>{
    e.preventDefault();
    const fd = new FormData(e.target);
    const payload = { member_id: fd.get('member_id'), principal: parseInt(fd.get('principal')), date_issued: fd.get('date_issued') || null };
    try {
      const res = await fetch(`${API}/loans/apply`, { method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
      const j = await res.json();
      if(res.ok) { alert('Loan applied. Due: ' + j.due_date); e.target.reset(); fetchLoans(); }
      else alert(JSON.stringify(j));
    } catch(err) {
      await addOutbox({ type: 'loan_apply', payload }); alert('Offline — queued');
    }
  };

  document.getElementById('member-report-form').onsubmit = async e=>{
    e.preventDefault();
    const fd = new FormData(e.target);
    const member_id = fd.get('member_id'), year = fd.get('year'), month = fd.get('month');
    const res = await fetch(`${API}/reports/member_monthly?member_id=${member_id}&year=${year}&month=${month}`);
    const j = await res.json();
    document.getElementById('member-report-output').textContent = JSON.stringify(j, null, 2);
  };

  document.getElementById('group-report-form').onsubmit = async e=>{
    e.preventDefault();
    const fd = new FormData(e.target);
    const year = fd.get('year'), month = fd.get('month');
    const res = await fetch(`${API}/reports/group_monthly?year=${year}&month=${month}`);
    const j = await res.json();
    document.getElementById('group-report-output').textContent = JSON.stringify(j, null, 2);
  };

  document.getElementById('eoy-btn').onclick = async ()=>{
    const yearInput = document.getElementById('eoy-year');
    const year = yearInput.value || new Date().getFullYear();
    const res = await fetch(`${API}/reports/eoy_distribution?year=${year}`);
    const j = await res.json();
    document.getElementById('eoy-output').textContent = JSON.stringify(j, null, 2);
  };
}

async function trySync(){
  if (!navigator.onLine) return;
  const items = await getOutboxAll();
  if(!items.length) { fetchMembers(); fetchLoans(); return; }
  for(const it of items){
    try {
      if(it.type === 'member_create') {
        await fetch(`${API}/members/`, { method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(it.payload) });
      } else if(it.type === 'contrib_create') {
        await fetch(`${API}/contributions/`, { method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(it.payload) });
      } else if(it.type === 'loan_apply') {
        await fetch(`${API}/loans/apply`, { method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(it.payload) });
      }
    } catch(err) {
      console.warn('sync err', err);
      return; // stop and retry later
    }
  }
  await clearOutbox();
  fetchMembers();
  fetchLoans();
}

async function fetchMembers(){
  try {
    const res = await fetch(`${API}/members/`);
    const j = await res.json();
    const ul = document.getElementById('members-list'); ul.innerHTML = '';
    j.forEach(m => { const li = document.createElement('li'); li.textContent = `${m.id} - ${m.name}`; ul.appendChild(li); });
  } catch(err) { console.warn('fetchMembers', err); }
}

async function fetchLoans(){
  try {
    const res = await fetch(`${API}/loans/`);
    const j = await res.json();
    const wrap = document.getElementById('loans-list'); wrap.innerHTML = '';
    j.forEach(l => {
      const d = document.createElement('div');
      d.innerHTML = `<strong>Loan ${l.id} — Member ${l.member_id}</strong><br>
        Principal: ${l.principal} | Interest: ${l.interest} | Penalty: ${l.penalty} | Remaining: ${l.remaining_principal} | Total Now: ${l.total_due_now}`;
      wrap.appendChild(d);
    });
  } catch(err) { console.warn('fetchLoans', err); }
}

window.addEventListener('load', ()=>{ init().catch(console.error); });
