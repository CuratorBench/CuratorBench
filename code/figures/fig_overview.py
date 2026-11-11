"""Shared-metrics overview for WWW."""
from __future__ import annotations

from fig_layout import (
    BLUE, INK, INK2, LINE, NAVY, OUT, PALE, ROOT,
    arrow, box, canvas, human, line, save, text,
)
from functools import lru_cache
import hashlib
import json
import shutil
import matplotlib as mpl
from matplotlib.image import imread
from matplotlib.patches import Circle, Polygon, Rectangle

BLUE = "#386F9F"
NAVY = "#2E516F"
PALE = "#ECF2F8"
LINE = "#C8D3DC"
WARM = "#A56D36"
WARM_PALE = "#F7EFE3"
DATA = "#2C7E73"
DATA_PALE = "#EAF4F0"
MODEL = "#705698"
MODEL_PALE = "#F1EDF7"
SLATE = "#405D6B"
YIELD = "#2C8068"
YIELD_PALE = "#EDF5F0"
RISK = "#B75C4D"
RISK_PALE = "#F9EEEB"
GREY = "#f8f8f7"


def route(ax, points, color=INK2, dashed=False, width=.8):
    xs, ys = zip(*points[:-1])
    if len(xs) > 1:
        ax.plot(xs, ys, color=color, linewidth=width,
                linestyle=(0, (2, 1.8)) if dashed else "-")
    arrow(ax, points[-2], points[-1], color, dashed=dashed)


TOKEN_DIR = ROOT / "figures/assets/token-icons"


@lru_cache(maxsize=4)
def token_image(name):
    return imread(TOKEN_DIR / f"{name}.png")


def token_icon(ax, x, y, name, height=12):
    artwork = token_image(name)
    width = height * artwork.shape[1] / artwork.shape[0]
    ax.imshow(artwork, extent=(x-width/2, x+width/2, y+height/2, y-height/2),
              aspect="auto", interpolation="antialiased", zorder=6)


def usdc(ax, x, y, r=6):
    token_icon(ax,x,y,"usdc",2*r)


def collateral(ax, x, y, kind, size=6):
    token_icon(ax,x,y,{"ETH":"eth","BTC":"btc","sUSDe":"susde"}[kind],2*size)


def safe(ax, x, y, scale=1, color=NAVY):
    box(ax,x-10*scale,y-8*scale,20*scale,16*scale,"white",color,radius=1.5*scale,lw=.8)
    ax.add_patch(Circle((x+1.7*scale,y),3.4*scale,facecolor="white",edgecolor=color,lw=.7))
    for dx,dy in ((3.4,0),(-3.4,0),(0,3.4),(0,-3.4)):
        line(ax,x+1.7*scale,y,x+(1.7+dx)*scale,y+dy*scale,color,.65)
    line(ax,x-6*scale,y-4*scale,x-6*scale,y+4*scale,color,.8)


def document(ax, x, y, scale=1):
    ax.add_patch(Polygon([(x+a*scale,y+b*scale) for a,b in
                         [(-7,-9),(3,-9),(7,-5),(7,9),(-7,9)]],
                         facecolor="white",edgecolor=DATA,lw=.7))
    line(ax,x+3*scale,y-9*scale,x+3*scale,y-5*scale,DATA,.6)
    line(ax,x+3*scale,y-5*scale,x+7*scale,y-5*scale,DATA,.6)
    for dy in (-2,2,6):
        line(ax,x-4*scale,y+dy*scale,x+4*scale,y+dy*scale,DATA,.55)


def chain_logs(ax, x, y, scale=1):
    for dx,dy in ((-9,-5),(0,0),(9,5)):
        box(ax,x+(dx-4)*scale,y+(dy-4)*scale,8*scale,8*scale,
            "white",DATA,radius=.6,lw=.65)
    line(ax,x-5*scale,y-3*scale,x-4*scale,y-2*scale,DATA,.7)
    line(ax,x+4*scale,y+2*scale,x+5*scale,y+3*scale,DATA,.7)


def market_data(ax, x, y, scale=1):
    for dx,top,bottom in ((-8,-5,7),(-1,-9,3),(7,-6,8)):
        line(ax,x+dx*scale,y+top*scale,x+dx*scale,y+bottom*scale,DATA,.65)
        ax.add_patch(Rectangle((x+(dx-1.7)*scale,y+(top+2)*scale),
                               3.4*scale,(bottom-top-4)*scale,
                               facecolor=DATA if dx<5 else "#9FC7BC",edgecolor="none"))


def compact_robot(ax, x, y):
    box(ax,x-6,y-4.3,12,8.6,"white",MODEL,radius=1.5,lw=.75)
    line(ax,x,y-4.3,x,y-6.3,MODEL,.75)
    ax.add_patch(Circle((x,y-7),.8,facecolor=MODEL,edgecolor="none"))
    for dx in (-2.3,2.3):
        ax.add_patch(Circle((x+dx,y-.5),.7,facecolor=MODEL,edgecolor="none"))
    line(ax,x-2,y+2,x+2,y+2,MODEL,.65)


def metrics(ax):
    loss_color = RISK
    loss_fill = RISK_PALE
    box(ax,170,1,171,138,"white",SLATE,radius=1,lw=.8)
    ax.add_patch(Rectangle((170.5,1.5),170,18.5,facecolor=SLATE,edgecolor="none"))
    text(ax,255.5,10.5,"CuratorBench",9.1,"white",True,"center")
    line(ax,255,20,255,139,LINE,.6)
    line(ax,170,79,341,79,LINE,.6)
    for x,y,w,fill in ((170,20,85,YIELD_PALE),(255,20,86,RISK_PALE),
                       (170,79,85,PALE),(255,79,86,MODEL_PALE)):
        ax.add_patch(Rectangle((x+.5,y+.5),w-1,16,facecolor=fill,edgecolor="none"))
    for x,y,label,color in ((212.5,29,"Realized yield",YIELD),
                            (298,29,"Stress tail loss",RISK),
                            (212.5,88,"Liquidity",NAVY),
                            (298,88,"Event outcomes",MODEL)):
        text(ax,x,y,label,8.1,color,True,"center")

    text(ax,179,46,"%",7.1,INK2,ha="center")
    line(ax,187,41,187,65,LINE,.6)
    line(ax,187,65,247,65,LINE,.6)
    xs=[188,199,211,222,232,245]
    ys=[62,59,53,55,47,42]
    ax.fill_between(xs,ys,65,color=YIELD_PALE)
    ax.plot(xs,ys,color=YIELD,lw=1.1)
    text(ax,216,72,"Time",7.1,INK2,ha="center")

    text(ax,260,45,"Before",7.1,INK2)
    text(ax,260,57,"Stress",7.1,INK2)
    ax.add_patch(Rectangle((288,41),44,7,facecolor=BLUE,edgecolor="none"))
    ax.add_patch(Rectangle((288,53),26,7,facecolor=BLUE,edgecolor="none"))
    ax.add_patch(Rectangle((314,53),18,7,facecolor=loss_fill,edgecolor=loss_color,
                           hatch="////",linewidth=.45))
    line(ax,323,60,323,64,loss_color,.6)
    text(ax,323,71,"Loss",7.1,loss_color,ha="center")

    ax.add_patch(Rectangle((180,106),40,9,facecolor=BLUE,edgecolor="none"))
    ax.add_patch(Rectangle((220,106),25,9,facecolor="#eceef0",edgecolor="#7d8790",
                           hatch="////",linewidth=.45))
    text(ax,200,125,"Available",7.1,NAVY,ha="center")
    text(ax,232.5,125,"Locked",7.1,INK2,ha="center")

    for y,label,unit in ((103,"Exit lead","days"),(115,"Principal loss","%"),
                          (127,"Lock-up","days")):
        text(ax,262,y,label,7.1,INK2)
        text(ax,333,y,unit,7.1,MODEL,ha="right")
        if y != 127:
            line(ax,262,y+6,333,y+6,LINE,.45)


def draw():
    fig, ax = canvas(504, 142)
    box(ax,2,1,154,138,"#fafaf9",LINE,radius=1)
    ax.add_patch(Rectangle((2.5,1.5),153,18.5,facecolor=WARM_PALE,edgecolor="none"))
    text(ax,79,10.5,"Curation layer",9.1,WARM,True,"center")
    line(ax,2,20,156,20,LINE,.6)
    box(ax,355,1,147,138,"white",LINE,radius=1)

    box(ax,61,25,75,18,WARM_PALE,"#D8BC98",radius=1)
    human(ax,71,34,WARM,scale=.52)
    text(ax,106,34,"Human curator",7.8,WARM,True,"center")
    arrow(ax,(100,44),(100,65),WARM,dashed=True)
    text(ax,122,55,"Controls",7.1,WARM,ha="center")
    arrow(ax,(90,65),(90,44),WARM)
    text(ax,80,55,"Fee",7.1,WARM,ha="center")
    human(ax,19,71,scale=.65)
    text(ax,21,90,"Depositors",7.2,ha="center")
    arrow(ax,(38,77),(77,77),BLUE)
    usdc(ax,54,64,6)
    text(ax,54,53,"USDC",7.2,BLUE,True,"center")
    box(ax,78,66,47,22,"white",INK2,radius=1,lw=.65)
    safe(ax,87,77,.42)
    text(ax,109,77,"Vault",8.1,bold=True,ha="center")

    line(ax,100,88,100,94,INK2,.75)
    box(ax,5,102,147,28,"#f2f3f2","#deded9",radius=1,lw=.5)
    text(ax,78.5,107,"Collateral",6.9,INK2,ha="center")
    for x,token in ((29,"ETH"),(78,"BTC"),(127,"sUSDe")):
        arrow(ax,(100,94),(x,101),INK2)
        box(ax,x-23,112,46,17,"white",LINE,radius=.7,lw=.55)
        collateral(ax,x-15,120.5,token,6)
        text(ax,x+5,120.5,token,7.2,NAVY,ha="center")
    text(ax,78.5,135,"Isolated lending markets",7.2,INK2,ha="center")
    arrow(ax,(126,77),(169,77),WARM)

    metrics(ax)

    box(ax,355,1,147,53,"white",DATA,radius=1,lw=.75)
    ax.add_patch(Rectangle((355.5,1.5),146,16.5,facecolor=DATA_PALE,edgecolor="none"))
    text(ax,428.5,10,"Public data",8.6,DATA,True,"center")
    line(ax,355,18,502,18,DATA,.55)
    line(ax,404,18,404,43,LINE,.5)
    line(ax,453,18,453,43,LINE,.5)
    document(ax,379.5,25,.6)
    chain_logs(ax,428.5,25,.65)
    market_data(ax,477.5,25,.65)
    text(ax,379.5,38,"Documents",7,INK2,ha="center")
    text(ax,428.5,38,"On-chain data",7,INK2,ha="center")
    text(ax,477.5,38,"Market data",7,INK2,ha="center")
    ax.add_patch(Rectangle((355.5,43),146,10.5,facecolor=DATA_PALE,edgecolor="none"))
    line(ax,355,43,502,43,DATA,.5)
    text(ax,428.5,49,"State / oracles / dated corpus",6.9,DATA,ha="center")

    arrow(ax,(381,55),(381,64),DATA)
    arrow(ax,(445.5,55),(445.5,64),DATA)
    box(ax,360,65,42,28,WARM_PALE,WARM,radius=1,lw=.65)
    text(ax,381,75,"Baselines",7.5,WARM,True,"center")
    text(ax,381,87,"rules / passive",6.4,INK2,ha="center")
    box(ax,407,65,77,28,MODEL_PALE,MODEL,radius=1,lw=.75)
    compact_robot(ax,420,76)
    text(ax,452,75,"AutoCurator",8,MODEL,True,"center")
    line(ax,408,82,483,82,"#D8CEE5",.5)
    text(ax,445.5,88,"List / read docs",7,MODEL,ha="center")

    arrow(ax,(370,94),(370,109),WARM)
    arrow(ax,(461,94),(461,109),MODEL)
    text(ax,454,102,"Allocation",7.1,MODEL,ha="right")
    arrow(ax,(416,109),(416,94),BLUE)
    text(ax,410,102,"Holdings",7.1,BLUE,ha="right")
    arrow(ax,(492,55),(492,109),DATA)

    box(ax,361,110,134,27,PALE,BLUE,radius=1,lw=.75)
    text(ax,428,118,"Historical replay",8,NAVY,True,"center")
    line(ax,365,123,491,123,LINE,.5)
    text(ax,380,130,"Withdraw",7.1,INK2,ha="center")
    text(ax,426,130,"Supply",7.1,INK2,ha="center")
    text(ax,474,130,"Accrue",7.1,INK2,ha="center")
    arrow(ax,(398,130),(411,130),BLUE)
    arrow(ax,(440,130),(456,130),BLUE)
    route(ax,[(360,124),(348,124),(348,77),(342,77)],BLUE)

    with mpl.rc_context({"savefig.dpi":1200,"image.composite_image":False}):
        save(fig,"overview","Compact overview of curation, shared metrics, and policy replay")
    for suffix in ("pdf", "svg", "png"):
        shutil.copy2(OUT / f"overview.{suffix}", ROOT / "figures" / f"fig1_overview.{suffix}")
    sources = [ROOT / "code/figures/fig_overview.py",
               ROOT / "code/figures/fig_layout.py",
               ROOT / "code/figures/style.py", TOKEN_DIR / "sources.json"]
    sources.extend(sorted(TOKEN_DIR.glob("*.svg")))
    sources.extend(sorted(TOKEN_DIR.glob("*.png")))
    metadata = {
        "kind": "schematic overview; no measured values or model results",
        "dimensions_pt": [504, 142],
        "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in sources},
        "human_path": "recorded holdings enter the shared metrics directly",
        "policy_path": "baseline and AutoCurator allocations use historical replay",
        "token_artwork": "original sources and asset hashes in figures/assets/token-icons/sources.json",
    }
    (ROOT / "figures/fig1_overview.pdf.meta.json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    OUT.mkdir(parents=True,exist_ok=True)
    draw()
    print(f"WWW Figure 1: {ROOT / 'figures/fig1_overview.pdf'} (504 x 142 pt)")
