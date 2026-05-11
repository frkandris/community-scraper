#!/usr/bin/env python3
"""Generate 1200x630 branded OG images for each topic + default.

Run from repo root:
    python scripts/gen_og_images.py

Output: scraper/web/static/img/og/
Requires: pillow>=10.1  (pip install -e ".[dev]")
"""

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

W, H = 1200, 630
OUT_DIR = Path(__file__).parent.parent / "scraper" / "web" / "static" / "img" / "og"
_BRAND = (168, 81, 47)  # #A8512F

TOPIC_LABELS: dict[str, str] = {
    "running":           "Running / Futás",
    "board_games":       "Board Games / Társasjáték",
    "choir":             "Choir / Kórus",
    "dance":             "Dance / Tánc",
    "cycling":           "Cycling / Kerékpározás",
    "hiking":            "Hiking / Túrázás",
    "yoga":              "Yoga / Jóga",
    "photography":       "Photography / Fotózás",
    "book_club":         "Book Club / Könyvklub",
    "chess":             "Chess / Sakk",
    "cooking":           "Cooking / Főzés",
    "theater":           "Theater / Színjátszás",
    "music":             "Music / Zene",
    "martial_arts":      "Martial Arts / Harcművészet",
    "gaming":            "Gaming / Videójáték",
    "volunteering":      "Volunteering / Önkéntesség",
    "language_exchange": "Language Exchange / Nyelvcsere",
    "art":               "Art / Képzőművészet",
    "meditation":        "Meditation / Meditáció",
    "swimming":          "Swimming / Úszás",
    "hagyomanyorzes":    "Hagyományőrzés",
    "gardening":         "Gardening / Kertészet",
    "film_club":         "Film Club / Filmklub",
    "trivia":            "Trivia & Quizzes / Kvíz",
    "sustainability":    "Sustainability / Fenntarthatóság",
    "crafts":            "Crafts & Making / Kézimunka",
    "fitness":           "Fitness",
    "religion":          "Religion & Faith / Hitközösség",
    "baby":              "Baba & Szülő",
    "senior":            "Seniors / Nyugdíjas",
    "kisallat":          "Kisállat",
    "other":             "Other / Egyéb",
    "default":           "közösségek.com",
}

TOPIC_BG: dict[str, tuple[int, int, int]] = {
    "running":           (155,  55,  25),
    "board_games":       ( 85,  65, 130),
    "choir":             (125,  45,  90),
    "dance":             (165,  45, 100),
    "cycling":           ( 35,  95, 140),
    "hiking":            ( 55, 105,  55),
    "yoga":              ( 95,  75, 150),
    "photography":       ( 45,  45,  65),
    "book_club":         (105,  65,  35),
    "chess":             ( 35,  35,  55),
    "cooking":           (175,  75,  25),
    "theater":           (115,  25,  55),
    "music":             ( 45,  65, 150),
    "martial_arts":      ( 75,  25,  25),
    "gaming":            ( 55,  35, 120),
    "volunteering":      ( 35, 115,  75),
    "language_exchange": ( 35,  95, 130),
    "art":               (145,  55, 130),
    "meditation":        ( 75,  95, 140),
    "swimming":          ( 25,  95, 160),
    "hagyomanyorzes":    (115,  75,  25),
    "gardening":         ( 45, 105,  45),
    "film_club":         ( 55,  25,  75),
    "trivia":            (165, 115,  25),
    "sustainability":    ( 35, 115,  55),
    "crafts":            (155,  75,  55),
    "fitness":           (135,  35,  35),
    "religion":          ( 85,  65, 110),
    "baby":              (175,  95,  75),
    "senior":            ( 95,  75,  55),
    "kisallat":          ( 55, 105,  85),
    "other":             ( 95,  85,  75),
    "default":           _BRAND,
}


def _gradient_fill(draw: ImageDraw.ImageDraw, bg: tuple[int, int, int]) -> None:
    r, g, b = bg
    dr, dg, db = max(0, r - 35), max(0, g - 35), max(0, b - 35)
    for y in range(H):
        t = y / H
        draw.line(
            [(0, y), (W, y)],
            fill=(int(r + (dr - r) * t), int(g + (dg - g) * t), int(b + (db - b) * t)),
        )


def _wrap(text: str, max_chars: int = 22) -> list[str]:
    words = text.split()
    lines: list[str] = []
    cur = ""
    for w in words:
        test = (cur + " " + w).strip()
        if len(test) > max_chars and cur:
            lines.append(cur)
            cur = w
        else:
            cur = test
    if cur:
        lines.append(cur)
    return lines


def _make_image(topic: str, label: str) -> Image.Image:
    bg = TOPIC_BG.get(topic, _BRAND)
    img = Image.new("RGB", (W, H), color=bg)
    draw = ImageDraw.Draw(img, "RGBA")
    _gradient_fill(draw, bg)
    draw.ellipse([W - 230, -90, W + 90, 230], fill=(255, 255, 255, 18))
    draw.ellipse([-90, H - 190, 190, H + 90], fill=(255, 255, 255, 12))
    try:
        f_site = ImageFont.load_default(size=28)
    except TypeError:
        f_site = ImageFont.load_default()
    draw.text((52, 48), "közösségek.com", font=f_site, fill=(255, 255, 255, 160))
    try:
        f_topic = ImageFont.load_default(size=72)
    except TypeError:
        f_topic = ImageFont.load_default()
    wrapped = _wrap(label, max_chars=22)
    line_h = 88
    y0 = (H - len(wrapped) * line_h) // 2 - 24
    for i, line in enumerate(wrapped):
        bbox = draw.textbbox((0, 0), line, font=f_topic)
        tw = bbox[2] - bbox[0]
        x = (W - tw) // 2
        y = y0 + i * line_h
        draw.text((x + 3, y + 3), line, font=f_topic, fill=(0, 0, 0, 70))
        draw.text((x, y), line, font=f_topic, fill=(255, 255, 255, 255))
    draw.rectangle([0, H - 64, W, H], fill=(0, 0, 0, 70))
    try:
        f_bottom = ImageFont.load_default(size=22)
    except TypeError:
        f_bottom = ImageFont.load_default()
    draw.text(
        (52, H - 44),
        "Találd meg a közösséged Magyarországon",
        font=f_bottom,
        fill=(255, 255, 255, 180),
    )
    return img


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for topic, label in TOPIC_LABELS.items():
        path = OUT_DIR / f"{topic}.png"
        _make_image(topic, label).save(path, "PNG", optimize=True)
        print(f"  wrote {path.name}")
    print(f"\nDone — {len(TOPIC_LABELS)} images written to {OUT_DIR}")


if __name__ == "__main__":
    main()
