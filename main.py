import asyncio
import hashlib
import json
import os
import re
import shutil
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

try:
    from opentele2.api import API, UseCurrentSession
    from opentele2.exception import TFileNotFound
    from opentele2.td import TDesktop
except ImportError:
    raise ImportError("pip install opentele2")

if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except (AttributeError, RuntimeError):
        pass

try:
    from src.config import API_ID, API_HASH
except ImportError:
    API_ID = int(os.getenv("API_ID", "0"))
    API_HASH = os.getenv("API_HASH", "")

if not API_ID or not API_HASH:
    raise ValueError("API_ID and API_HASH required")


class C:
    def __init__(self, a=None, h=None, sd="sessions", ud="uploads", bd="tdata_backup", mr=2, rd=10, to=60, p=3):
        self.a = a or API_ID
        self.h = h or API_HASH
        self.sd = Path(sd)
        self.ud = Path(ud)
        self.bd = Path(bd)
        self.mr = mr
        self.rd = rd
        self.to = to
        self.p = p
        self.s = asyncio.Semaphore(p)
        self.e = ThreadPoolExecutor(max_workers=p)
        self.c = {}
        self._lc = asyncio.Lock()
        self._pr = self._lp()
        self._ac = self._ca()

        for d in (self.sd, self.ud, self.bd):
            d.mkdir(exist_ok=True)

    def _ca(self):
        return type("A", (API,), {"api_id": int(self.a), "api_hash": str(self.h)})

    def _lp(self):
        r = os.getenv("PROXY") or os.getenv("PROXIES")
        if not r:
            return None
        try:
            import socks
        except ImportError:
            return None
        r = r.strip()
        if "://" in r:
            p = urlparse(r)
            s = p.scheme.lower()
            t = {"socks5": socks.SOCKS5, "socks4": socks.SOCKS4, "http": socks.HTTP}
            pt = t.get(s, socks.SOCKS5)
            if p.username and p.password:
                return (pt, p.hostname, p.port or 1080, True, p.username, p.password)
            return (pt, p.hostname, p.port or 1080)
        parts = r.split(":")
        if len(parts) < 2:
            return None
        try:
            port = int(parts[1])
        except ValueError:
            return None
        if len(parts) >= 4 and parts[2] and parts[3]:
            return (socks.SOCKS5, parts[0], port, True, parts[2], parts[3])
        return (socks.SOCKS5, parts[0], port)

    @staticmethod
    def _np(r: str) -> Optional[str]:
        c = re.sub(r"[^0-9]", "", r)
        if c and len(c) >= 7:
            return "+" + c
        return None

    def _gp(self, p: Path, h: str = None) -> str:
        if h:
            ph = self._np(h)
            if ph:
                return ph
        ph = self._np(p.parent.name)
        if ph:
            return ph
        return f"+{hashlib.md5(str(p.resolve()).encode()).hexdigest()[:8]}"

    def _vt(self, p: Path) -> Tuple[bool, Optional[Path]]:
        if not p.exists() or not p.is_dir():
            return False, None
        kf = list(p.glob("key_datas"))
        if not kf:
            for sd in p.iterdir():
                if sd.is_dir():
                    kf = list(sd.glob("key_datas"))
                    if kf:
                        return True, kf[0].parent
            return False, None
        return True, kf[0].parent

    def _gt(self, p: Path):
        k = str(p.resolve())
        if k in self.c:
            return self.c[k]

        def _l():
            try:
                td = TDesktop(str(p))
                if td.isLoaded() and td.accounts:
                    return td
                return None
            except Exception:
                return None

        f = self.e.submit(_l)
        td = f.result(timeout=self.to)
        if td:
            self.c[k] = td
        return td

    def _sb(self, p: Path, ph: str) -> Path:
        bf = self.bd / ph.replace("+", "")
        bt = bf / "tdata"
        if bt.exists():
            return bt
        bt.mkdir(parents=True, exist_ok=True)
        for pat in ["key_datas", "map*", "config*"]:
            for f in p.glob(pat):
                if f.is_file():
                    shutil.copy2(f, bt / f.name)
        m = {"phone": ph, "saved_at": datetime.now().isoformat(), "source": str(p)}
        with open(bf / "meta.json", "w", encoding="utf-8") as f:
            json.dump(m, f, indent=2, ensure_ascii=False)
        return bt

    async def _co(self, p: Path, ph: str, att: int = 1) -> Dict:
        ok, rp = self._vt(p)
        if not ok:
            return {"ok": False, "err": "Invalid tdata"}
        sf = self.sd / f"{ph}.session"
        if sf.exists() and sf.stat().st_size > 10:
            try:
                if len(sf.read_text(encoding="utf-8")) > 10:
                    return {"ok": True, "sf": str(sf), "ex": True}
            except:
                sf.unlink(missing_ok=True)
        self._sb(rp, ph)
        td = self._gt(rp)
        if not td:
            return {"ok": False, "err": "TDesktop load failed"}
        cl = None
        try:
            cl = await asyncio.wait_for(
                td.ToTelethon(
                    session=StringSession(),
                    flag=UseCurrentSession,
                    api=self._ac,
                    proxy=self._pr,
                    auto_reconnect=False,
                ),
                timeout=self.to,
            )
            await asyncio.wait_for(cl.connect(), timeout=15)
            if not await cl.is_user_authorized():
                await cl.disconnect()
                return {"ok": False, "err": "Not authorized"}
            me = await asyncio.wait_for(cl.get_me(), timeout=10)
            if not me:
                return {"ok": False, "err": "No user data"}
            ss = cl.session.save()
            sf.write_text(ss, encoding="utf-8")
            mt = {
                "phone": ph,
                "id": me.id,
                "username": me.username,
                "first_name": me.first_name,
                "last_name": me.last_name,
                "premium": getattr(me, "premium", False),
                "created": datetime.now().isoformat(),
            }
            (self.sd / f"{ph}.meta").write_text(json.dumps(mt, indent=2, ensure_ascii=False), encoding="utf-8")
            await cl.disconnect()
            return {"ok": True, "sf": str(sf), "user": mt}
        except asyncio.TimeoutError:
            if cl:
                await cl.disconnect()
            return {"ok": False, "err": "Timeout"}
        except FloodWaitError as e:
            if cl:
                await cl.disconnect()
            if att < self.mr:
                w = min(e.seconds, 30) + 5
                await asyncio.sleep(w)
                return await self._co(p, ph, att + 1)
            return {"ok": False, "err": f"Flood {e.seconds}"}
        except Exception as e:
            if cl:
                await cl.disconnect()
            return {"ok": False, "err": str(e)}

    async def cf(self, fp: Path, ph: str = None) -> Tuple[bool, Dict]:
        if not ph:
            ph = self._gp(fp)
        if not fp.exists():
            return False, {"phone": ph, "err": "Path not found"}
        async with self.s:
            r = await self._co(fp, ph)
        if r.get("ok"):
            return True, {"phone": ph, "sf": r.get("sf"), "user": r.get("user")}
        return False, {"phone": ph, "err": r.get("err", "Error")}

    async def cfb(self, fl: List[Tuple[Path, str]]) -> List[Dict]:
        ts = [self.cf(fp, ph) for fp, ph in fl]
        rs = await asyncio.gather(*ts, return_exceptions=True)
        out = []
        for i, r in enumerate(rs):
            if isinstance(r, Exception):
                out.append({"phone": fl[i][1], "ok": False, "err": str(r)})
            elif isinstance(r, tuple):
                ok, data = r
                out.append({"phone": data.get("phone", fl[i][1]), "ok": ok, **data})
        return out

    async def czb(self, zf) -> Dict:
        r = {"ok": [], "fail": [], "total": 0}
        zp = None
        ep = None
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            zp = self.ud / f"{ts}_{zf.filename or 'archive.zip'}"
            zp.write_bytes(await zf.read())
            ep = self.ud / f"extract_{ts}"
            ep.mkdir(exist_ok=True)
            with zipfile.ZipFile(zp, "r") as z:
                z.extractall(ep)
            tps = []
            for root, dirs, _ in os.walk(ep):
                if "tdata" in dirs:
                    tps.append(Path(root) / "tdata")
            r["total"] = len(tps)
            if not tps:
                return {"ok": [], "fail": [], "total": 0, "err": "No tdata"}
            tsks = [(tp, self._gp(tp, zf.filename)) for tp in tps]
            rs = await self.cfb(tsks)
            for cr in rs:
                if cr.get("ok"):
                    r["ok"].append(cr)
                else:
                    r["fail"].append(cr)
        except Exception as e:
            return {"ok": [], "fail": [], "total": 0, "err": str(e)}
        finally:
            if ep and ep.exists():
                shutil.rmtree(ep, ignore_errors=True)
            if zp and zp.exists():
                zp.unlink(missing_ok=True)
        return r

    def cc(self):
        self.c.clear()


async def cli():
    import argparse
    p = argparse.ArgumentParser(description="tdata -> .session")
    p.add_argument("path", help="Path to tdata folder")
    p.add_argument("-p", "--phone", help="Phone number (optional)")
    p.add_argument("-t", "--timeout", type=int, default=60, help="Timeout in seconds")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = p.parse_args()

    if args.verbose:
        import logging
        logging.basicConfig(level=logging.DEBUG)

    path = Path(args.path)
    if not path.exists():
        print(f"❌ Path not found: {path}")
        return

    conv = C(to=args.timeout)
    ok, res = await conv.cf(path, args.phone)

    if ok:
        print("\nSUCCESS")
        print(f"Phone: {res['phone']}")
        print(f"   Session: {res['sf']}")
        if res.get("user"):
            u = res["user"]
            print(f"   User: {u.get('first_name')} @{u.get('username')}")
    else:
        print(f"\n❌ Error: {res.get('err')}")


if __name__ == "__main__":
    asyncio.run(cli())