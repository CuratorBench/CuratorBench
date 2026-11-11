"""Compose a styled figure from real Morpho screenshots, without retyping data."""

from pathlib import Path
import zlib
from PIL import Image, ImageDraw

FIGURES = Path(__file__).resolve().parents[2] / "figures"
SOURCE = FIGURES / "src"
CAPTURE_SIZE = (2600, 1920)
PRINT_WIDTH_IN = 3.34
OUTPUT_WIDTH_PX = 2400
LAYOUT_WIDTH = 1200
OUTER_PAD = 24
CONTENT_WIDTH = LAYOUT_WIDTH - 2 * OUTER_PAD
CANVAS_COLOR = (255, 255, 255)
CARD_COLORS = ((231, 240, 255), (233, 244, 255), (236, 237, 255), (221, 246, 238))
TABLE_HEAD_COLOR = (222, 234, 251)
TABLE_ROW_COLORS = ((255, 255, 255), (241, 247, 255))
CARD_Y, CARD_HEIGHT, CARD_GAP = 76, 138, 16
TABLE_Y, TABLE_HEAD_HEIGHT, TABLE_ROW_HEIGHT = 230, 48, 64
TABLE_INSET = 16

HEADER_FILE = "morpho_vault_header.png"
TITLE_CROP = (44, 532, 1128, 632)
TITLE_WIDTH_PX = 450
IDENTITY_CROPS = ((338, 675, 509, 730), (543, 675, 844, 730))
METRIC_COLUMNS = ((44, 350), (452, 774), (870, 1220), (1338, 1568))
METRIC_LABEL_CROPS = ((44, 1004, 243, 1048), (452, 1004, 617, 1048),
                      (870, 1004, 1008, 1048), (1338, 1004, 1498, 1048))
METRIC_VALUE_BAND = (1068, 1152)
METRIC_VALUE_HEIGHT = 55
METRIC_SECONDARY_HEIGHT = 24
METRIC_SECONDARY_CROPS = ((44, 1186, 229, 1216), (452, 1186, 650, 1216), None, None)
MARKET_COLUMNS = (
    ("morpho_vault_markets.png", (84, 394), (244, 554)),
    ("morpho_vault_markets.png", (606, 906), (606, 906)),
    ("morpho_vault_markets.png", (966, 1170), (966, 1170)),
    ("morpho_vault_markets_apy.png", (1338, 1446), (1338, 1446)),
)
TABLE_HEADER_BAND = (598, 636)
TABLE_ROW_BANDS = ((674, 724), (762, 812), (850, 900))
TABLE_STRIP_PX = 1000
RAW_BACKGROUND = (250, 252, 255)


def join(images, horizontal=False):
    """Paste adjoining crops without gaps or borders."""
    width = sum(im.width for im in images) if horizontal else images[0].width
    height = images[0].height if horizontal else sum(im.height for im in images)
    assert all((im.height if horizontal else im.width) ==
               (height if horizontal else width) for im in images)
    result = Image.new("RGB", (width, height))
    offset = 0
    for im in images:
        result.paste(im, (offset, 0) if horizontal else (0, offset))
        offset += im.width if horizontal else im.height
    return result


def resize_width(im, width):
    return im.resize((width, round(im.height * width / im.width)),
                     Image.Resampling.LANCZOS)


def tinted_background(im, color):
    """Replace the screenshot's pale blue background, retaining foreground ink."""
    pixels = bytearray(im.tobytes())
    for i in range(0, len(pixels), 3):
        r, g, b = pixels[i:i + 3]
        if (max(abs(r - 250), abs(g - 252), abs(b - 255)) <= 10
                and 1 <= g - r <= 3 and 2 <= b - g <= 4):
            pixels[i:i + 3] = bytes(max(0, min(255, c + delta))
                                    for c, delta in zip(color, (r - 250, g - 252, b - 255)))
    return Image.frombytes("RGB", im.size, pixels)


def write_pdf(im, profile, path):
    """Embed one lossless image on a precisely sized, marginless PDF page."""
    width = PRINT_WIDTH_IN * 72
    height = width * im.height / im.width

    def stream(data, attributes=b""):
        return (b"<< " + attributes + f" /Length {len(data)} >>\nstream\n".encode()
                + data + b"\nendstream")

    content = f"q {width:.8f} 0 0 {height:.8f} 0 0 cm /Image Do Q\n".encode()
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {width:.8f} {height:.8f}] "
         "/Resources << /XObject << /Image 4 0 R >> >> /Contents 5 0 R >>").encode(),
        stream(zlib.compress(im.tobytes(), 9),
               (f"/Type /XObject /Subtype /Image /Width {im.width} /Height {im.height} "
                "/ColorSpace [/ICCBased 6 0 R] /BitsPerComponent 8 "
                "/Interpolate false /Filter /FlateDecode").encode()),
        stream(content),
        stream(zlib.compress(profile, 9), b"/N 3 /Alternate /DeviceRGB /Filter /FlateDecode"),
    ]
    with path.open("wb") as out:
        out.write(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
        offsets = [0]
        for number, obj in enumerate(objects, 1):
            offsets.append(out.tell())
            out.write(f"{number} 0 obj\n".encode() + obj + b"\nendobj\n")
        xref = out.tell()
        out.write(f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode())
        for offset in offsets[1:]:
            out.write(f"{offset:010d} 00000 n \n".encode())
        out.write((f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\n"
                   f"startxref\n{xref}\n%%EOF\n").encode())


def main():
    names = {HEADER_FILE, *(name for name, _, _ in MARKET_COLUMNS)}
    captures = {name: Image.open(SOURCE / name) for name in names}
    assert all(im.size == CAPTURE_SIZE for im in captures.values())
    profile = captures[HEADER_FILE].info["icc_profile"]
    assert all(im.info["icc_profile"] == profile for im in captures.values())

    def crop(name, box):
        return captures[name].crop(box).convert("RGB")

    height = TABLE_Y + TABLE_HEAD_HEIGHT + len(TABLE_ROW_BANDS) * TABLE_ROW_HEIGHT + 14
    canvas = Image.new("RGB", (LAYOUT_WIDTH, height), CANVAS_COLOR)
    title = resize_width(tinted_background(crop(HEADER_FILE, TITLE_CROP), CANVAS_COLOR),
                         TITLE_WIDTH_PX)
    canvas.paste(title, (OUTER_PAD, 17))
    ethereum, curator = [tinted_background(crop(HEADER_FILE, box), CANVAS_COLOR)
                         for box in IDENTITY_CROPS]
    curator_x = LAYOUT_WIDTH - OUTER_PAD - curator.width
    canvas.paste(ethereum, (curator_x - ethereum.width - 16, 13))
    canvas.paste(curator, (curator_x, 13))

    draw = ImageDraw.Draw(canvas)
    card_width = (CONTENT_WIDTH - 3 * CARD_GAP) // 4
    for i, ((left, right), label_box, secondary_box, color) in enumerate(zip(
            METRIC_COLUMNS, METRIC_LABEL_CROPS, METRIC_SECONDARY_CROPS, CARD_COLORS,
            strict=True)):
        x = OUTER_PAD + i * (card_width + CARD_GAP)
        draw.rounded_rectangle((x, CARD_Y, x + card_width - 1, CARD_Y + CARD_HEIGHT - 1),
                               radius=10, fill=color)
        canvas.paste(tinted_background(crop(HEADER_FILE, label_box), color), (x + 14, CARD_Y + 8))
        value = tinted_background(crop(HEADER_FILE, (left, METRIC_VALUE_BAND[0],
                                                    right, METRIC_VALUE_BAND[1])), color)
        value = value.resize((round(value.width * METRIC_VALUE_HEIGHT / value.height),
                              METRIC_VALUE_HEIGHT), Image.Resampling.LANCZOS)
        canvas.paste(value, (x + 14, CARD_Y + 47))
        if secondary_box:
            secondary = tinted_background(crop(HEADER_FILE, secondary_box), color)
            secondary = secondary.resize((round(secondary.width * METRIC_SECONDARY_HEIGHT
                                                / secondary.height), METRIC_SECONDARY_HEIGHT),
                                         Image.Resampling.LANCZOS)
            canvas.paste(secondary, (x + 14, CARD_Y + 105))

    def table_band(top, bottom, heading=False):
        cells = [crop(name, ((header_x if heading else body_x)[0], top,
                             (header_x if heading else body_x)[1], bottom))
                 for name, header_x, body_x in MARKET_COLUMNS]
        gap = (TABLE_STRIP_PX - sum(c.width for c in cells)) / (len(cells) - 1)
        assert gap >= 0
        band = Image.new("RGB", (TABLE_STRIP_PX, bottom - top), RAW_BACKGROUND)
        x = 0.0
        for c in cells:
            band.paste(c, (round(x), 0))
            x += c.width + gap
        return band

    table_height = TABLE_HEAD_HEIGHT + len(TABLE_ROW_BANDS) * TABLE_ROW_HEIGHT
    table = Image.new("RGB", (CONTENT_WIDTH, table_height), TABLE_HEAD_COLOR)
    table_draw = ImageDraw.Draw(table)
    header = resize_width(tinted_background(table_band(*TABLE_HEADER_BAND, heading=True),
                                            TABLE_HEAD_COLOR), CONTENT_WIDTH - 2 * TABLE_INSET)
    table.paste(header, (TABLE_INSET, (TABLE_HEAD_HEIGHT - header.height) // 2))
    for i, (top, bottom) in enumerate(TABLE_ROW_BANDS):
        color = TABLE_ROW_COLORS[i % 2]
        y = TABLE_HEAD_HEIGHT + i * TABLE_ROW_HEIGHT
        table_draw.rectangle((0, y, CONTENT_WIDTH, y + TABLE_ROW_HEIGHT - 1), fill=color)
        row = resize_width(tinted_background(table_band(top, bottom), color),
                           CONTENT_WIDTH - 2 * TABLE_INSET)
        table.paste(row, (TABLE_INSET, y + (TABLE_ROW_HEIGHT - row.height) // 2))
    mask = Image.new("L", table.size)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, table.width - 1, table.height - 1),
                                           radius=8, fill=255)
    canvas.paste(table, (OUTER_PAD, TABLE_Y), mask)
    composite = resize_width(canvas, OUTPUT_WIDTH_PX)
    dpi = OUTPUT_WIDTH_PX / PRINT_WIDTH_IN
    composite.save(FIGURES / "fig0_frontend.png", icc_profile=profile, dpi=(dpi, dpi))
    write_pdf(composite, profile, FIGURES / "fig0_frontend.pdf")
    print(f"{composite.width} x {composite.height} pixels; "
          f"{PRINT_WIDTH_IN:.2f} x {PRINT_WIDTH_IN * composite.height / composite.width:.3f} inches")


if __name__ == "__main__":
    main()
