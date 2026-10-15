import json, base64, struct, os, collections, hashlib, shutil, sys
import pyarrow as pa, pyarrow.parquet as pq
ALPH='123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
def b58d(s):
    n=0
    for c in s: n=n*58+ALPH.index(c)
    b=n.to_bytes((n.bit_length()+7)//8,'big') if n else b''
    return b'\0'*(len(s)-len(s.lstrip('1')))+b
def b58e(b):
    n=int.from_bytes(b,'big'); out=''
    while n: n,r=divmod(n,58); out=ALPH[r]+out
    return '1'*(len(b)-len(b.lstrip(b'\0')))+out
TOK={"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA","TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"}; SYS="11111111111111111111111111111111"; WSOL="So11111111111111111111111111111111111111112"
VOTE="Vote111111111111111111111111111111111111111"; CB="ComputeBudget111111111111111111111111111111"
CPI_PREFIX=bytes.fromhex('e445a52e51cb9a1d')
# ---- IDLs
IDLS={ 'meteora_dlmm':'/root/solana/protocols/meteora/dlmm-sdk/idls/dlmm.json','meteora_damm_v2':'/root/solana/protocols/meteora/zap-program/idls/damm_v2.json',
 'raydium_clmm':'dl/raydium_clmm.json','raydium_cpmm':'dl/raydium_cp_swap.json','raydium_launchlab':'dl/raydium_launchpad.json',
 'pumpswap':'dl/pump_amm.json','pumpfun':'dl/pump.json','orca_whirlpool':'dl/orca/package/dist/artifacts/whirlpool.json'}
PROG={}  # program_id -> (label, types, events{disc:(name,fields)})
for label,path in IDLS.items():
    d=json.load(open(path)); T={t['name']:t for t in d.get('types',[])}
    ev={}
    for e in d['events']:
        disc=bytes(e['discriminator']) if 'discriminator' in e else hashlib.sha256(b'event:'+e['name'].encode()).digest()[:8]
        t=T.get(e['name']); fields=t['type'].get('fields',[]) if t else e.get('fields',[])
        ev[disc]=(e['name'],fields)
    PROG[d['address']]=(label,T,ev)
RAY_V4="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
TIER1=set(PROG)|{RAY_V4}
jup=json.load(open('dl/jup_labels.json'))
ROUTERS={"JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4","61DFfeTKM7trxYcPQCM78bJ794ddZprZpAwAnLiwTpYH","fd3nMFYTQjX1yr5ER8u7tPdHJB7qt8RpDpNtLQX2Br5","routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS","T1TANpTeScyeqVzzgNViGDNrkQ6qHz9KrSBS4aNXvGT"}
EXCL={k for k,v in jup.items() if any(x in v for x in ('Sanctum','Jupiter Lend','JupiterRfq','Perps','Helium','Solayer','XOrca','VaultLiquidUnstake','Carrot','Voltr','Huma','Perena'))}
TIER2=set(jup)-TIER1-ROUTERS-EXCL
VENUES=TIER1|TIER2
def rd(ty,b,o,T):
    if isinstance(ty,str):
        if ty=='pubkey': return b58e(b[o:o+32]),o+32
        if ty=='bool': return bool(b[o]),o+1
        f={'u8':'<B','i8':'<b','u16':'<H','i16':'<h','u32':'<I','i32':'<i','u64':'<Q','i64':'<q','f64':'<d','f32':'<f'}
        if ty in f: n=struct.calcsize(f[ty]); return struct.unpack_from(f[ty],b,o)[0],o+n
        if ty in('u128','i128'): return int.from_bytes(b[o:o+16],'little',signed=ty=='i128'),o+16
        if ty=='string': n=struct.unpack_from('<I',b,o)[0]; return b[o+4:o+4+n].decode(errors='replace'),o+4+n
        if ty=='bytes': n=struct.unpack_from('<I',b,o)[0]; return b[o+4:o+4+n].hex(),o+4+n
        raise Exception('type '+ty)
    if 'array' in ty:
        t,n=ty['array']; out=[]
        for _ in range(n): v,o=rd(t,b,o,T); out.append(v)
        return out,o
    if 'vec' in ty:
        n=struct.unpack_from('<I',b,o)[0]; o+=4; out=[]
        for _ in range(n): v,o=rd(ty['vec'],b,o,T); out.append(v)
        return out,o
    if 'option' in ty:
        f=b[o]; o+=1
        return (None,o) if not f else rd(ty['option'],b,o,T)
    if 'defined' in ty:
        d=T[ty['defined']['name']]['type']
        if d['kind']=='struct':
            out={}
            for f in d['fields']: out[f['name']],o=rd(f['type'],b,o,T)
            return out,o
        if d['kind']=='enum': return d['variants'][b[o]]['name'],o+1
    raise Exception('type '+json.dumps(ty))
def decode_fields(fields,b,T):
    out={}; o=0
    for f in fields: out[f['name']],o=rd(f['type'],b,o,T)
    if o!=len(b): raise Exception('length mismatch')
    return out
def paths(tx):
    msg=tx['transaction']['message']; meta=tx['meta']; la=meta.get('loadedAddresses') or {}
    keys=[k if isinstance(k,str) else k['pubkey'] for k in msg['accountKeys']]+la.get('writable',[])+la.get('readonly',[])
    inner={x['index']:x['instructions'] for x in (meta.get('innerInstructions') or [])}
    out=[]
    for i,ix in enumerate(msg['instructions']):
        out.append((str(i),keys[ix['programIdIndex']],[keys[a] for a in ix['accounts']],ix['data']))
        counters=[]
        for iix in inner.get(i,[]):
            d=(iix.get('stackHeight') or 2)-2
            counters=counters[:d+1]
            while len(counters)<d+1: counters.append(0)
            counters[d]+=1
            out.append((str(i)+'.'+'.'.join(map(str,counters)),keys[iix['programIdIndex']],[keys[a] for a in iix['accounts']],iix['data']))
    return out,keys
ATA="ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
def tokmeta(tx,keys,px):
    m={}
    for tb in (tx['meta'].get('preTokenBalances') or [])+(tx['meta'].get('postTokenBalances') or []):
        m[keys[tb['accountIndex']]]=(tb['mint'],tb.get('owner'))
    for path,prog,accs,data in px:   # accounts created inside the tx never appear in balance metadata
        raw=b58d(data)
        if prog in TOK and raw:
            if raw[0]==1 and len(accs)>=3 and accs[0] not in m: m[accs[0]]=(accs[1],accs[2])            # InitializeAccount: [account, mint, owner]
            if raw[0] in(16,18) and len(accs)>=2 and accs[0] not in m: m[accs[0]]=(accs[1],b58e(raw[1:33]))  # InitializeAccount2/3: owner in data
        if prog==ATA and len(accs)>=4 and accs[1] not in m: m[accs[1]]=(accs[3],accs[2])                   # create ATA: [payer, ata, wallet, mint]
    return m
def transfers(px,tm,prefix):
    out=[]
    for path,prog,accs,data in px:
        if prefix is not None and not (path==prefix or path.startswith(prefix+'.')): continue
        if prefix is not None and path==prefix: continue
        raw=b58d(data)
        if prog in TOK and raw and raw[0] in(3,12) and len(raw)>=9:
            amt=struct.unpack_from('<Q',raw,1)[0]; src,dst=(accs[0],accs[1]) if raw[0]==3 else (accs[0],accs[2])
            ms,os_=tm.get(src,(None,None)); md,od=tm.get(dst,(None,None))
            if raw[0]==12 and ms is None: ms=accs[1]
            auth=accs[2] if raw[0]==3 else accs[3]
            if os_ is None: os_=auth
            if od is None: od=os_ if (md is None and ms is None) else od
            out.append((path,src,dst,ms or md,amt,os_,od))
        elif prog==SYS and len(raw)>=12 and struct.unpack_from('<I',raw,0)[0]==2:
            out.append((path,accs[0],accs[1],WSOL,struct.unpack_from('<Q',raw,4)[0],accs[0],accs[1]))
    return out
def net(trs):
    moved=collections.defaultdict(lambda: collections.defaultdict(int)); gross=collections.defaultdict(lambda: collections.defaultdict(lambda:[0,0]))
    for path,src,dst,mint,amt,os_,od in trs:
        moved[os_][mint]-=amt; moved[od][mint]+=amt; gross[os_][mint][0]+=amt; gross[od][mint][1]+=amt
    return moved,gross
def trader_rule(px,tm,prefix,signers,txdelta):
    trs=transfers(px,tm,prefix); moved,gross=net(trs)
    parties=[o for o,m in moved.items() if o and sum(1 for v in m.values() if v<0)==1 and sum(1 for v in m.values() if v>0)==1]
    user=None
    sp=[o for o in parties if o in signers]
    if len(sp)==1: user=sp[0]
    elif len(parties)>=1 and not sp:
        outside={a for t in transfers(px,tm,None) if not (t[0]==prefix or t[0].startswith(prefix+'.')) for a in (t[1],t[2])}
        cand=[o for o in parties if any(t[1] in outside or t[2] in outside for t in trs if o in (t[5],t[6]))]
        if len(cand)==1: user=cand[0]
    trader=user if user in signers else None
    if trader is None:
        sd=[o for o in signers if any(o==oo and d!=0 for (oo,m),d in txdelta.items())]
        if len(sd)==1: trader=sd[0]
    return trs,moved,gross,parties,user,trader
def txdeltas(tx,keys):
    pre=collections.Counter(); post=collections.Counter()
    for tb in tx['meta'].get('preTokenBalances') or []: pre[(tb.get('owner'),tb['mint'])]+=int(tb['uiTokenAmount']['amount'])
    for tb in tx['meta'].get('postTokenBalances') or []: post[(tb.get('owner'),tb['mint'])]+=int(tb['uiTokenAmount']['amount'])
    return {k:post[k]-pre[k] for k in set(pre)|set(post)}
def log_events(logs):
    """yield (program, path, payload_bytes) for Program data lines, with ray_log too"""
    stack=[]; counters=[]; out=[]
    for l in logs:
        if l.startswith('Program ') and ' invoke [' in l:
            p=l.split()[1]; d=len(stack)  # depth index
            counters=counters[:d+1]
            while len(counters)<d+1: counters.append(0)
            counters[d]+=1
            path=str(counters[0]-1)+(('.'+'.'.join(map(str,counters[1:]))) if len(counters)>1 else '')
            stack.append((p,path))
        elif l.startswith('Program ') and (l.endswith(' success') or ' failed' in l or l.endswith(' consumed')):
            if (l.endswith(' success') or ' failed: ' in l) and stack: stack.pop()
        elif l.startswith('Program data: ') and stack:
            out.append((stack[-1][0],stack[-1][1],base64.b64decode(l[14:])))
        elif l.startswith('Program log: ray_log: ') and stack:
            out.append((stack[-1][0],stack[-1][1],b'RAYLOG'+base64.b64decode(l[22:])))
    return out
tables=collections.defaultdict(list)
def envelope(sig,slot,bt,ti,path,ord_,outer,trader): return {'signature':sig,'slot':slot,'block_time':bt,'tx_index':ti,'ix_path':path,'event_ordinal':ord_,'outer_program':outer,'trader':trader}
blocks=json.load(open('sample_blocks.json'))
nblocks=0; stats=collections.Counter()
for bl in blocks:
    b=bl['block']; slot=bl['slot']
    if b is None: tables['blocks'].append({'slot':slot,'block_time':None,'status':'skipped'}); continue
    nblocks+=1; bt=b['blockTime']; tables['blocks'].append({'slot':slot,'block_time':bt,'status':'ok'})
    for ti,tx in enumerate(b['transactions']):
        if tx['meta'].get('err') is not None: continue
        px,keys=paths(tx); progs={p for _,p,_,_ in px}; outer_prog={i:keys[ix['programIdIndex']] for i,ix in enumerate(tx['transaction']['message']['instructions'])}
        if VOTE in progs or not (progs&VENUES): continue
        stats['venue_tx']+=1
        sig=tx['transaction']['signatures'][0]; n=tx['transaction']['message']['header']['numRequiredSignatures']; signers=set(keys[:n]); tm=tokmeta(tx,keys,px); td=txdeltas(tx,keys)
        outer_of=lambda path: outer_prog[int(path.split('.')[0])]
        # trader per venue instruction (cache)
        tr_cache={}
        def trader_for(path):
            if path not in tr_cache: tr_cache[path]=trader_rule(px,tm,path,signers,td)
            return tr_cache[path]
        # ---- tier 1 events: CPI
        ords=collections.Counter()
        for path,prog,accs,data in px:
            if prog in PROG:
                raw=b58d(data)
                if raw[:8]==CPI_PREFIX:
                    label,T,ev=PROG[prog]; parent='.'.join(path.split('.')[:-1]) or path
                    if raw[8:16] in ev:
                        name,fields=ev[raw[8:16]]
                        try:
                            f=decode_fields(fields,raw[16:],T); o=outer_of(parent); trs,mv,gr,parties,user,trader=trader_for(parent)
                            k=(parent,name); row=envelope(sig,slot,bt,ti,parent,ords[k],None if o==prog else o,trader); ords[k]+=1; row.update(f)
                            tables[f'events/{label}/{name}'].append(row)
                        except Exception as e:
                            tables['decode_failures'].append({'signature':sig,'slot':slot,'ix_path':parent,'program':prog,'reason':'decode:'+str(e)[:40],'ix_data':raw,'log_lines':[],'transfers':None})
                    else:
                        tables['decode_failures'].append({'signature':sig,'slot':slot,'ix_path':parent,'program':prog,'reason':'unknown_disc','ix_data':raw,'log_lines':[],'transfers':None})
        # ---- tier 1 events: logs (Program data + ray_log)
        for prog,path,payload in log_events(tx['meta'].get('logMessages') or []):
            if prog in PROG and payload[:8] in PROG[prog][2]:
                label,T,ev=PROG[prog]; name,fields=ev[payload[:8]]
                try:
                    f=decode_fields(fields,payload[8:],T); o=outer_of(path); trs,mv,gr,parties,user,trader=trader_for(path)
                    k=(path,name); row=envelope(sig,slot,bt,ti,path,ords[k],None if o==prog else o,trader); ords[k]+=1; row.update(f)
                    tables[f'events/{label}/{name}'].append(row)
                except Exception as e:
                    tables['decode_failures'].append({'signature':sig,'slot':slot,'ix_path':path,'program':prog,'reason':'decode:'+str(e)[:40],'ix_data':payload,'log_lines':[],'transfers':None})
            elif prog==RAY_V4 and payload.startswith(b'RAYLOG'):
                p=payload[6:]; lt=p[0]; vals=[struct.unpack_from('<Q',p,1+8*i)[0] for i in range((len(p)-1)//8)]
                name={0:'Init',1:'Deposit',2:'Withdraw',3:'SwapBaseIn',4:'SwapBaseOut'}.get(lt,'Other')
                o=outer_of(path); trs,mv,gr,parties,user,trader=trader_for(path)
                ixaccs=[x for x in px if x[0]==path]; amm=ixaccs[0][2][1] if ixaccs and len(ixaccs[0][2])>1 else None
                row=envelope(sig,slot,bt,ti,path,0,None if o==prog else o,trader); row['amm']=amm
                for i,v in enumerate(vals): row[f'f{i}']=v
                tables[f'events/raydium_amm_v4/{name}'].append(row)
        # ---- tier 2 swaps
        for path,prog,accs,data in px:
            if prog in TIER2:
                o=outer_of(path); trs,mv,gr,parties,user,trader=trader_for(path)
                if user is not None:
                    m=mv[user]; mint_in=[k for k,v in m.items() if v<0][0]; mint_out=[k for k,v in m.items() if v>0][0]
                    row=envelope(sig,slot,bt,ti,path,0,None if o==prog else o,trader); row.update({'program':prog,'mint_in':mint_in,'amount_in':-m[mint_in],'mint_out':mint_out,'amount_out':m[mint_out]})
                    tables[f'swaps/{jup.get(prog,prog)}'].append(row)
                elif trs and parties:
                    tables['decode_failures'].append({'signature':sig,'slot':slot,'ix_path':path,'program':prog,'reason':'not_a_swap','ix_data':b58d(data),'log_lines':[],'transfers':[{'source':t[1],'destination':t[2],'mint':t[3],'amount':t[4],'owner_source':t[5],'owner_destination':t[6]} for t in trs]})
                elif trs: stats['tier2_non_swap_ix']+=1
                else: stats['tier2_no_transfers']+=1
        # ---- intents
        outers=sorted({int(p.split('.')[0]) for p,prog,_,_ in px if prog in VENUES})
        for i in outers:
            prefix=str(i); trs,mv,gr,parties,user,trader=trader_rule(px,tm,prefix,signers,td)
            if not trs: stats['outer_no_transfers']+=1; continue
            if not parties: stats['outer_no_swap_party']+=1; continue
            # fee legs: transfers from the trader to a pure recipient (receives only, never sends, not a hop party) are excluded
            senders={t[5] for t in trs}; hop_parties=set()
            for p,prog,_,_ in px:
                if prog in VENUES and (p==prefix or p.startswith(prefix+'.')):
                    for t in transfers(px,tm,p): hop_parties.add(t[5]); hop_parties.add(t[6])
            core=[t for t in trs if not (t[6] not in senders and t[6] not in hop_parties)]
            mv,gr=net(core)
            hop_mints={t[3] for p,prog,_,_ in px if prog in VENUES and (p==prefix or p.startswith(prefix+'.')) for t in transfers(px,tm,p)}
            who=trader or user
            if who is None or who not in mv: tables['decode_failures'].append({'signature':sig,'slot':slot,'ix_path':prefix,'program':outer_prog[i],'reason':'not_an_intent','ix_data':b'','log_lines':[],'transfers':[{'source':t[1],'destination':t[2],'mint':t[3],'amount':t[4],'owner_source':t[5],'owner_destination':t[6]} for t in trs]}); continue
            m={k:v for k,v in mv[who].items() if v!=0 and k in hop_mints}; neg=[k for k,v in m.items() if v<0]; pos=[k for k,v in m.items() if v>0]
            hops=sum(1 for p,prog,_,_ in px if prog in VENUES and (p==prefix or p.startswith(prefix+'.')))
            if len(neg)==1 and len(pos)==1: kind='swap'; mi,mo=neg[0],pos[0]
            elif len(m)<=1 and len(gr[who])>=1: kind='cycle'; mi=mo=max(gr[who],key=lambda k:gr[who][k][0])
            else:
                tables['decode_failures'].append({'signature':sig,'slot':slot,'ix_path':prefix,'program':outer_prog[i],'reason':'not_an_intent','ix_data':b'','log_lines':[],'transfers':[{'source':t[1],'destination':t[2],'mint':t[3],'amount':t[4],'owner_source':t[5],'owner_destination':t[6]} for t in trs]}); continue
            row=envelope(sig,slot,bt,ti,prefix,0,None,trader); row.update({'program':outer_prog[i],'mint_in':mi,'amount_in':gr[who][mi][0],'mint_out':mo,'amount_out':gr[who][mo][1],'hops':hops,'kind':kind})
            tables['intents'].append(row)
# ---- write parquet
def flatten(row,prefix=''):
    out={}
    for k,v in row.items():
        if isinstance(v,dict): out.update(flatten(v,prefix+k+'.'))
        elif isinstance(v,list) and v and isinstance(v[0],dict): out[prefix+k]=json.dumps(v)
        else: out[prefix+k]=v
    return out
def col(vals):
    for t in (None,pa.uint64(),pa.string()):
        try:
            if t==pa.string(): vals=[None if v is None else (json.dumps(v) if isinstance(v,(list,dict)) else str(v)) for v in vals]
            return pa.array(vals,type=t)
        except Exception: continue
    return pa.array([None]*len(vals))
shutil.rmtree('out',ignore_errors=True); os.makedirs('out')
sizes={}
for name,rows in tables.items():
    rows=[flatten(r) for r in rows]; cols=sorted({k for r in rows for k in r})
    tbl=pa.table({c:col([r.get(c) for r in rows]) for c in cols})
    p='out/'+name.replace('/','__')+'.parquet'; pq.write_table(tbl,p,compression='zstd',compression_level=3); sizes[name]=(len(rows),os.path.getsize(p))
per_day=216000/nblocks
print(f"blocks={nblocks} venue_tx={stats['venue_tx']} per block={stats['venue_tx']/nblocks:.1f}")
groups=collections.defaultdict(lambda:[0,0])
for name,(n,sz) in sizes.items():
    g=name.split('/')[0] if not name.startswith('events/') else 'events/'+name.split('/')[1]
    groups[g][0]+=n; groups[g][1]+=sz
print(f"{'table group':34s} {'rows/60blk':>10s} {'bytes':>9s} {'B/row':>6s} {'rows/day':>10s} {'GB/day':>7s} {'GB/90d':>7s}")
tot=[0,0]
for g,(n,sz) in sorted(groups.items(),key=lambda x:-x[1][1]):
    print(f"{g:34s} {n:10d} {sz:9d} {sz/max(n,1):6.0f} {n*per_day/1e6:8.1f}M {sz*per_day/1e9:7.2f} {sz*per_day*90/1e9:7.1f}")
    tot[0]+=n; tot[1]+=sz
print(f"{'TOTAL':34s} {tot[0]:10d} {tot[1]:9d} {'':6s} {tot[0]*per_day/1e6:8.1f}M {tot[1]*per_day/1e9:7.2f} {tot[1]*per_day*90/1e9:7.1f}")
print("top event tables:",sorted(((n,name) for name,(n,sz) in sizes.items() if name.startswith('events/')),reverse=True)[:8])
print("decode_failures by reason:",collections.Counter(r['reason'].split(':')[0] for r in tables['decode_failures']).most_common(6))
print("decode errors by (program,event):",collections.Counter((PROG[r['program']][0],PROG[r['program']][2].get(r['ix_data'][8:16] if r['ix_data'][:8]==CPI_PREFIX else r['ix_data'][:8],('?',))[0],r['reason'][:30]) for r in tables['decode_failures'] if r['reason'].startswith('decode') and r['program'] in PROG).most_common(6))
print("skipped: ",dict(stats)); print("not_an_intent residual:",sum(1 for r in tables['decode_failures'] if r['reason']=='not_an_intent'))
print("intents trader resolution:",collections.Counter('null' if r['trader'] is None else 'set' for r in tables['intents']))
print("intents kinds:",collections.Counter(r['kind'] for r in tables['intents']),"trader null:",sum(1 for r in tables['intents'] if r['trader'] is None))
