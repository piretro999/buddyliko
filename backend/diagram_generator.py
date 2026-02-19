"""
diagram_generator.py
Genera un diagramma SVG della mappatura — puro Python, zero dipendenze esterne.
"""

from typing import List, Dict, Any, Optional
import xml.etree.ElementTree as ET


# ── Palette colori ─────────────────────────────────────────────────────────────
BG          = "#f8f9fa"
HEADER_BG   = "#1a1a2e"
HEADER_FG   = "#ffffff"
COL_L_BG    = "#e8f4fd"
COL_R_BG    = "#e8fdf0"
COL_L_BORDER= "#2196f3"
COL_R_BORDER= "#4caf50"
ROW_ALT     = "#f0f8ff"
ROW_EVEN    = "#ffffff"
ARROW_COLOR = "#666666"
TRANS_BADGE = "#ff9800"
TEXT_MAIN   = "#1a1a2e"
TEXT_MUTED  = "#666666"
TEXT_PATH   = "#888888"
SECTION_BG  = "#eef2ff"
SECTION_FG  = "#3949ab"

# ── Layout ─────────────────────────────────────────────────────────────────────
MARGIN      = 40
COL_W       = 340      # larghezza colonna source / target
MID_W       = 120      # larghezza colonna centrale (freccia + trasformazione)
ROW_H       = 52       # altezza riga
HEADER_H    = 80
TITLE_H     = 60
SECTION_H   = 28
TOTAL_W     = MARGIN*2 + COL_W + MID_W + COL_W
FONT        = "Segoe UI, Arial, sans-serif"


def _escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _section_label(path: str) -> str:
    """Estrae la sezione logica dal path sorgente."""
    if not path:
        return "Altro"
    parts = path.split("/")
    if len(parts) >= 2:
        return parts[1]  # es. DatiGenerali, DatiBeniServizi, etc.
    return parts[0]


def _short_path(path: str) -> str:
    """Abbrevia il path per la visualizzazione."""
    if not path:
        return ""
    parts = path.split("/")
    if len(parts) > 2:
        return "…/" + "/".join(parts[-2:])
    return path


def _target_label(target_path: str) -> str:
    """Prende l'ultimo elemento significativo del target path."""
    if not target_path:
        return ""
    # Rimuovi attributi (@xxx)
    path = target_path.split("/@")[0]
    parts = path.split("/")
    return parts[-1].replace("cbc:", "").replace("cac:", "")


def generate_svg(connections: List[Dict[str, Any]],
                 project_name: str = "Mappatura",
                 input_schema_name: str = "Input",
                 output_schema_name: str = "Output") -> str:
    """
    Genera SVG completo della mappatura.
    Restituisce stringa SVG.
    """
    # Filtra connessioni valide
    valid = [c for c in connections if c.get("source") and c.get("target")]

    # Raggruppa per sezione sorgente
    sections: Dict[str, List[Dict]] = {}
    for conn in valid:
        sec = _section_label(conn.get("sourcePath", ""))
        sections.setdefault(sec, []).append(conn)

    # Calcola altezza totale
    n_sections = len(sections)
    n_rows = len(valid)
    total_h = TITLE_H + HEADER_H + n_sections * SECTION_H + n_rows * ROW_H + MARGIN * 2

    # ── SVG root ───────────────────────────────────────────────────────────────
    svg = ET.Element("svg", {
        "xmlns": "http://www.w3.org/2000/svg",
        "width": str(TOTAL_W),
        "height": str(total_h),
        "viewBox": f"0 0 {TOTAL_W} {total_h}",
        "font-family": FONT,
    })

    # Defs (freccia)
    defs = ET.SubElement(svg, "defs")
    marker = ET.SubElement(defs, "marker", {
        "id": "arrow",
        "markerWidth": "10",
        "markerHeight": "7",
        "refX": "10",
        "refY": "3.5",
        "orient": "auto",
    })
    ET.SubElement(marker, "polygon", {
        "points": "0 0, 10 3.5, 0 7",
        "fill": ARROW_COLOR,
    })

    # Sfondo
    ET.SubElement(svg, "rect", {
        "width": str(TOTAL_W), "height": str(total_h),
        "fill": BG,
    })

    # ── Titolo ─────────────────────────────────────────────────────────────────
    ET.SubElement(svg, "rect", {
        "x": "0", "y": "0",
        "width": str(TOTAL_W), "height": str(TITLE_H),
        "fill": HEADER_BG,
    })
    t = ET.SubElement(svg, "text", {
        "x": str(TOTAL_W // 2), "y": "38",
        "text-anchor": "middle",
        "font-size": "20",
        "font-weight": "bold",
        "fill": HEADER_FG,
    })
    t.text = _escape(project_name)
    sub = ET.SubElement(svg, "text", {
        "x": str(TOTAL_W // 2), "y": "56",
        "text-anchor": "middle",
        "font-size": "12",
        "fill": "#aaaacc",
    })
    sub.text = f"{len(valid)} connessioni attive"

    # ── Header colonne ─────────────────────────────────────────────────────────
    hy = TITLE_H
    x_left  = MARGIN
    x_mid   = MARGIN + COL_W
    x_right = MARGIN + COL_W + MID_W

    # Colonna sinistra
    ET.SubElement(svg, "rect", {
        "x": str(x_left), "y": str(hy),
        "width": str(COL_W), "height": str(HEADER_H),
        "fill": COL_L_BG, "stroke": COL_L_BORDER, "stroke-width": "2",
        "rx": "6",
    })
    _text(svg, x_left + COL_W//2, hy + 28, input_schema_name, 15, "bold", COL_L_BORDER, "middle")
    _text(svg, x_left + COL_W//2, hy + 50, "Campo sorgente", 11, "normal", TEXT_MUTED, "middle")
    _text(svg, x_left + COL_W//2, hy + 66, "Path", 10, "normal", TEXT_PATH, "middle")

    # Colonna centrale
    _text(svg, x_mid + MID_W//2, hy + 40, "Trasformazione", 11, "normal", TEXT_MUTED, "middle")

    # Colonna destra
    ET.SubElement(svg, "rect", {
        "x": str(x_right), "y": str(hy),
        "width": str(COL_W), "height": str(HEADER_H),
        "fill": COL_R_BG, "stroke": COL_R_BORDER, "stroke-width": "2",
        "rx": "6",
    })
    _text(svg, x_right + COL_W//2, hy + 28, output_schema_name, 15, "bold", COL_R_BORDER, "middle")
    _text(svg, x_right + COL_W//2, hy + 50, "Campo target", 11, "normal", TEXT_MUTED, "middle")
    _text(svg, x_right + COL_W//2, hy + 66, "Path", 10, "normal", TEXT_PATH, "middle")

    # ── Righe ─────────────────────────────────────────────────────────────────
    y = TITLE_H + HEADER_H
    row_idx = 0

    for sec_name, conns in sections.items():
        # Intestazione sezione
        ET.SubElement(svg, "rect", {
            "x": str(MARGIN), "y": str(y),
            "width": str(TOTAL_W - MARGIN*2), "height": str(SECTION_H),
            "fill": SECTION_BG, "rx": "4",
        })
        _text(svg, MARGIN + 12, y + 18, f"▸  {_escape(sec_name)}", 11, "bold", SECTION_FG, "start")
        _text(svg, TOTAL_W - MARGIN - 8, y + 18, f"{len(conns)} campi", 10, "normal", SECTION_FG, "end")
        y += SECTION_H

        for conn in conns:
            # Sfondo riga alternato
            row_bg = ROW_ALT if row_idx % 2 == 0 else ROW_EVEN
            ET.SubElement(svg, "rect", {
                "x": str(MARGIN), "y": str(y),
                "width": str(TOTAL_W - MARGIN*2), "height": str(ROW_H),
                "fill": row_bg,
            })

            # Bordo leggero
            ET.SubElement(svg, "line", {
                "x1": str(MARGIN), "y1": str(y + ROW_H),
                "x2": str(TOTAL_W - MARGIN), "y2": str(y + ROW_H),
                "stroke": "#dddddd", "stroke-width": "1",
            })

            cy = y + ROW_H // 2  # centro riga

            # ── Cella sorgente ────────────────────────────────────────────────
            src_name = conn.get("source", "")
            src_path = _short_path(conn.get("sourcePath", ""))
            _text(svg, x_left + 12, cy - 6, _escape(src_name), 13, "600", TEXT_MAIN, "start")
            _text(svg, x_left + 12, cy + 10, _escape(src_path), 10, "normal", TEXT_PATH, "start")

            # BT badge se disponibile
            bt = conn.get("businessTerm", "")
            if bt:
                bw = len(bt) * 7 + 10
                ET.SubElement(svg, "rect", {
                    "x": str(x_left + COL_W - bw - 8), "y": str(cy - 11),
                    "width": str(bw), "height": "16",
                    "fill": "#e3f2fd", "rx": "8",
                })
                _text(svg, x_left + COL_W - bw//2 - 8, cy + 2, _escape(bt), 9, "normal", "#1565c0", "middle")

            # ── Freccia + trasformazione ───────────────────────────────────────
            trans = conn.get("transformation", {}) or {}
            trans_type = (trans.get("type") or "DIRECT").upper()

            ax1 = x_mid + 8
            ax2 = x_mid + MID_W - 8

            # Linea freccia
            ET.SubElement(svg, "line", {
                "x1": str(ax1), "y1": str(cy),
                "x2": str(ax2), "y2": str(cy),
                "stroke": ARROW_COLOR, "stroke-width": "1.5",
                "marker-end": "url(#arrow)",
            })

            # Badge tipo trasformazione
            if trans_type and trans_type != "DIRECT":
                bw2 = min(len(trans_type) * 7 + 10, MID_W - 10)
                bx = x_mid + MID_W//2 - bw2//2
                ET.SubElement(svg, "rect", {
                    "x": str(bx), "y": str(cy - 18),
                    "width": str(bw2), "height": "14",
                    "fill": TRANS_BADGE, "rx": "7",
                })
                _text(svg, x_mid + MID_W//2, cy - 8, _escape(trans_type), 8, "bold", "#ffffff", "middle")

            # ── Cella target ──────────────────────────────────────────────────
            tgt_name  = _target_label(conn.get("targetPath", conn.get("target", "")))
            tgt_path  = _short_path(conn.get("targetPath", ""))
            _text(svg, x_right + 12, cy - 6, _escape(tgt_name), 13, "600", TEXT_MAIN, "start")
            _text(svg, x_right + 12, cy + 10, _escape(tgt_path), 10, "normal", TEXT_PATH, "start")

            y += ROW_H
            row_idx += 1

    # ── Footer ─────────────────────────────────────────────────────────────────
    ET.SubElement(svg, "rect", {
        "x": "0", "y": str(total_h - 24),
        "width": str(TOTAL_W), "height": "24",
        "fill": HEADER_BG,
    })
    _text(svg, TOTAL_W//2, total_h - 9, "Buddyliko — Mapping Diagram", 10, "normal", "#aaaacc", "middle")

    return '<?xml version="1.0" encoding="utf-8"?>\n' + ET.tostring(svg, encoding="unicode")


def _text(parent, x, y, text, size, weight, color, anchor):
    """Helper: aggiunge elemento text SVG."""
    el = ET.SubElement(parent, "text", {
        "x": str(x), "y": str(y),
        "font-size": str(size),
        "font-weight": str(weight),
        "fill": color,
        "text-anchor": anchor,
    })
    el.text = text
    return el
