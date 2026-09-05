# Google-forgalom helyreállítása – 2026. szeptember 5.

Ez javasolt munkaterv, nem forgalmi előrejelzés. Kiinduló adatok és bizonyítékok:
[Search Console-vizsgálat](wiki/pages/seo/search-console-2026-09-05.md).

## Kiinduló helyzet

2026. augusztus 7.–szeptember 3., Google Web keresés:

| Webhely | Kattintás | Megjelenés |
|---|---:|---:|
| kozossegek.com | 2 | 14 |
| meetapedia.com | 53 | 1 247 |

Ez kattintásszám, nem egyedi látogató vagy munkamenet. Az indexelési jelentések
augusztus 28-iak, és a korábbi, átirányított URL-eket is tartalmazzák.

## 1. hét: technikai javítás és megbízható mérés

- Élesítés után ellenőrizni: a sitemapokban nincs `/community-general` témalap
  vagy átirányító `/about`, `/explore` bejegyzés. A régi témájú közösségi adatlapok
  megmaradtak, önmagukra mutató canonicallel.
- Mindkét domainnél a Search Console Feltérképezési statisztikák, hostállapot,
  kézi műveletek és biztonsági problémák áttekintése. A robots.txt letöltését és
  a tényleges Googlebot-kéréseket a szerver/Cloudflare naplóival is összevetni.
- 20 magyar, már újrafeltérképezett, nem indexelt URL és 10 nemzetközi,
  felfedezett URL ellenőrzése URL Inspectionnel. Rögzíteni: utolsó feltérképezés,
  Google által választott canonical, élő teszt, indexelési állapot, oldaltípus.
- Hetente ugyanazon 28 napos gördülő időszak exportja mindkét domainhez:
  kattintás, megjelenés, oldalak, keresések, országok. A fejlődést azonos URL-ekre
  és országokra is mérni, hogy az új oldalak ne fedjék el a régiek állapotát.
- A `/cities` kb. 1 MB-os HTML-jének külön teljesítményvizsgálata; az első
  válaszidőt, dokumentumméretet és Googlebot-hibaarányt külön mérni.

## 2–4. hét: kis, ellenőrzött magyar tartalmi kísérlet

A kozossegek.com legyen az elsődleges helyreállítási cél. Javasolt kezdőminta:
3 város × 3 keresési szándék, például kezdő futócsoport, társasjátékklub,
nyugdíjasklub. A végleges választást friss keresési adatok és a valóban elérhető
közösségek határozzák meg; ezek nem igazolt keresési volumen alapján választott
kulcsszavak.

- A meglévő város–téma oldalakból készíteni 9 használható összefoglalót, összesen
  kb. 30 ellenőrzött közösségi adatlappal. Meglévő URL-ek javítása, új párhuzamos
  landingoldalak létrehozása nélkül.
- Adatlaponként: tényleges település/cím, működő szervezet, jelentkezés módja,
  ismert időpont, díj vagy az ismeretlensége, célcsoport, forrás és ellenőrzési dátum.
  Amit nem tudunk, azt nem pótolja általános vagy kitalált szöveg.
- A város–téma összefoglaló segítsen választani: melyik csoport kezdőbarát,
  mikor lehet menni, hogyan lehet csatlakozni. A hivatkozott szervezeti oldalhoz
  képest az összehasonlíthatóság és az ellenőrzött gyakorlati információ az érték.
- Hasonló, változatlan oldalakból kontrollmintát kijelölni. A javított és a
  kontrollcsoport indexelését, megjelenéseit és kattintásait külön követni.
- Szervezőktől adatellenőrzést és valódi partnerséget kérni; saját oldalukról
  akkor hivatkozzanak az adatlapra, ha az hasznos nekik. Tömeges automatikus
  megkeresés, linkvásárlás és kötelező viszontlink nélkül. Ez a terv nem küldött
  üzenetet senkinek.

## 4–8. hét: értékelés és feltételes bővítés

Heti ellenőrzés, majd a 4. és 8. héten döntés:

| Megfigyelés | Következő lépés |
|---|---|
| Google még nem tért vissza a javított oldalakra | Hozzáférés, belső linkek, szerverterhelés és feltérképezési naplók vizsgálata |
| Visszatért, de a mintában sincs indexelési javulás | Canonical, tartalmi pontosság, hasznosság és duplikáció újraellenőrzése |
| Indexelés és releváns megjelenések nőnek | A működő oldaltípust fokozatosan több városra kiterjeszteni |
| Vannak releváns megjelenések, de kevés kattintás | Keresési szándék, cím és leírás vizsgálata a tényleges pozíció mellett |
| Van kattintás, de nincs szervezői kapcsolatfelvétel | Csatlakozási információ és mérés javítása |

Mérjük a megjelenést kapó különböző oldalak számát, a mintán belüli indexelési
arányt, releváns kereséseket és szervezőkhöz vezető kattintásokat is. A régi `/out`
átirányítós követést ne állítsuk vissza; az eredeti cél-URL-t megtartó eseménymérés
kell. A jelenlegi követés állapotát külön ellenőrizni kell az eredmények értékelése előtt.

A Meetapedián ezután egyetlen országgal és néhány várossal kezdenék, helyi nyelvű,
ellenőrzött összefoglalókkal. A jelenlegi német szöveg + angol témacím keverékét
a választott célközönséghez igazítanám, stabil URL-ekkel. A konkrét országot a friss,
országra szűrt adatok alapján kell kiválasztani.

## Amitől önmagában nem várnék helyreállást

Több tízezer új rekord; leírások általános meghosszabbítása; a teljes állomány
egyszerre történő újraírása; minden kizárt URL felszabadítása; ismételt sitemap-
beküldés változatlan oldalakkal. Nincs megalapozott határidő vagy garantált
forgalomszám a jelenlegi adatokból.

A terv a Google [hasznos tartalomra vonatkozó útmutatójára](https://developers.google.com/search/docs/fundamentals/creating-helpful-content)
és [feltérképezési hibakeresésére](https://developers.google.com/search/docs/crawling-indexing/troubleshoot-crawling-errors)
támaszkodik: a gyakorlati hozzáadott érték és a hozzáférhetőség együtt számít;
egy meghatározott szószám vagy pusztán gyorsabb kiszolgálás nem garantál indexelést.
