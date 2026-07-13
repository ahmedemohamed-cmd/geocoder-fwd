# Autocomplete recall report

- Base: `http://localhost:8000`
- Named cases: **750** → **2317** prefix probes (lengths [3, 5, 8] + first word)
- Geo-bias: downtown Cairo (30.0444, 31.2357), limit 10, lenient radius 150 m

## Named — overall

| metric | @1 | @5 | @10 |
|---|---|---|---|
| strict (osm_id) | 33.3% | 58.0% | 68.2% |
| lenient (name or ≤150 m) | 35.5% | 59.5% | 70.0% |

## Named — by prefix length

| prefix | probes | strict@1 | strict@5 | lenient@1 | redis share | p50 ms |
|---|---|---|---|---|---|---|
| len3 | 749 | 12.8% | 33.4% | 14.4% | 97.1% | 8.3 |
| len5 | 743 | 31.1% | 60.2% | 33.2% | 78.1% | 7.5 |
| len8 | 665 | 54.9% | 79.7% | 57.4% | 33.4% | 12.4 |
| word | 160 | 49.4% | 72.5% | 53.8% | 58.1% | 7.4 |

## Which backend answered

| source | probes | share | strict@1 within source |
|---|---|---|---|
| `redis` | 1622 | 70.0% | 17.0% |
| `elasticsearch` | 695 | 30.0% | 71.4% |

Latency: **p50 8.5 ms**, **p90 26.3 ms**

## Category queries

A type query should return places *of that type*. `hit rate` = share of top-5 whose ES category matches. Name-only matches (e.g. the "Metro" supermarket) do not count.

| query | source | hit rate | top-5 (category) |
|---|---|---|---|
| `metro` | elasticsearch | 60% | Metro *(shop/supermarket)*, Metro *(shop/supermarket)*, Bab el Shaaria *(railway/station)* |
| `metro station` | elasticsearch | 0% | Giza Metro Station *(amenity/bus_station)*, Dokki metro station, Metro Station Ataba |
| `مترو` | elasticsearch | 100% | Bulaq El-Dakroor *(railway/station)*, Rawd El-Farag *(railway/station)*, Bab el Shaaria *(railway/station)* |
| `hospital` | elasticsearch | 100% | Cairo University Hospitals *(amenity/hospital)*, Cairo Specialist Hospital (Cleopatra Hospitals) *(amenity/hospital)*, Queens Hospital _ Cleopatra Hospitals Group *(amenity/hospital)* |
| `مستشفى` | elasticsearch | 100% | Cairo University Hospitals *(amenity/hospital)*, مستشفى الشروق (مجموعة مستشفيات كليوباترا) *(amenity/hospital)*, مستشفي الهلال *(amenity/hospital)* |
| `cafe` | elasticsearch | 100% | Grand Cafèè *(amenity/cafe)*, Grand Cafèè *(amenity/cafe)*, Sehraya cafee *(amenity/cafe)* |
| `مقهى` | elasticsearch | 100% | مقهي *(amenity/cafe)*, مقهي احمد فرغلي *(amenity/cafe)*, مقهى ارابيسك *(amenity/cafe)* |
| `pharmacy` | elasticsearch | 100% | Fayek Pharmacy *(amenity/pharmacy)*, Fayek Pharmacy *(amenity/pharmacy)*, Masarra Pharmacy *(amenity/pharmacy)* |
| `صيدلية` | elasticsearch | 100% | صيدليهالجمال *(amenity/pharmacy)*, صيدليةد/ عمرو&نانسي *(amenity/pharmacy)*, صيدلية *(amenity/pharmacy)* |
| `school` | elasticsearch | 100% | Tiba Schools *(amenity/school)*, City International Schools *(amenity/school)*, Port Said Schools *(amenity/school)* |
| `مدرسة` | elasticsearch | 100% | مدرسه المكفوفين *(amenity/school)*, LYCEE VOLTAIRE *(amenity/school)*, EL ALSSON *(amenity/school)* |
| `mosque` | elasticsearch | 100% | Nile Mosque *(amenity/place_of_worship)*, mosque *(amenity/place_of_worship)*, mosque *(amenity/place_of_worship)* |
| `مسجد` | elasticsearch | 100% | مسجدا الرحمن *(amenity/place_of_worship)*, مسجدظ الصيرفى *(amenity/place_of_worship)*, مسجدآل البيت *(amenity/place_of_worship)* |
| `restaurant` | elasticsearch | 100% | Majesty Restaurants *(amenity/restaurant)*, Nile Lily Restaurants *(amenity/restaurant)*, Nile Lily Restaurants *(amenity/restaurant)* |
| `bank` | elasticsearch | 100% | Ahli United Bank *(amenity/bank)*, National Bank of Egypt *(amenity/bank)*, The United Bank *(amenity/bank)* |
| `supermarket` | elasticsearch | 100% | Nasser Supermarket *(shop/supermarket)*, Misho Supermarket *(shop/supermarket)*, Sunny Supermarket *(shop/supermarket)* |

**Mean category hit rate: 91.2%**
