# AGENTS_BAASTASE.md

Lisajuhised baastaseme (`baastase/`) praktikumimaterjalide loomiseks.
Kehtib koos failiga `AGENTS.md`.

## Sihtrühm

- Õppijal ei pruugi olla andmetehnika ega andmeinseneeria tausta.
- Ta võib olla tugev oma erialases töös, kuid talle võivad olla uued `SQL`, `Python`, käsurida, Docker, versioonihaldus ja pilveteenused.
- Ära eelda, et ta tunneb käsurea põhikäske. Selgita neid lühidalt.
- Kui juhend toetub varasemale praktikumikorrale, nimeta see selgelt ja meenuta vajalikud eelteadmised paari lausega.

## Repo kontekstis oluline

- Baastaseme materjal peab töötama ka iseseisvaks läbimiseks Moodle'is. Ära jäta kriitilisi samme ainult juhendaja suulise selgituse kanda.
- Selles repos kasutatakse sageli `VS Code`-i või `GitHub Codespaces`-it, terminali, Dockerit ja `psql`-i. Ütle alati selgelt, millises keskkonnas samm tehakse.
- Nimeta alati, kas tegevus toimub hostis, konteineris, `psql` sees või veebiliideses.
- Kui kaustas on valmis failid nagu `compose.yml`, `.env.example`, `scripts/` või `data/`, tutvusta neid enne kasutamist lühidalt.
- Kui õppijal on valida mitme töötee vahel, soovita üks vaikimisi tee. Algaja ei pea ise otsustama, milline tööviis on kõige mõistlikum.

## Didaktilised põhimõtted

- Üks oluline uus idee korraga. Vähem, aga paremini.
- Selgita mitte ainult "kuidas", vaid ka "miks".
- Seo tehniline tegevus tuttava tööprotsessi või olukorraga, näiteks Exceli tabeli, failikausta või ekspordifailiga.
- Kasuta vahekokkuvõtteid ja kontrollpunkte, et õppija ei kaotaks järge.
- Selgita iga sammu juures tausta, töövõtet ja oodatavat tulemust rohkem kui edasijõudnute materjalides.
- Ära jäta õppijat olukorda, kus ta peab midagi tegema "usu peale". Selgita põhjus.
- Kui uus mõiste ilmub sammu sees, selgita see kohe. Ära lükka selgitust mitu jaotist edasi.

## Baastaseme juhendis tasub enamasti lisada

- `Hinnanguline ajakulu`, sest õppija teeb kursust töö kõrvalt ja tahab oma aega planeerida.
- Lühike jaotis "Enne alustamist", eriti siis, kui juhend toetub eelmise nädala keskkonnale või oskustele.
- Jaotis "Praktikumi failid", et õppija saaks repo kaustastruktuuris kiiresti orienteeruda.
- Vajadusel eraldi selgitus hosti ja konteineri erinevuse kohta.
- Kontrollpunktid või vahetulemused, mis aitavad aru saada, kas töö edeneb õigesti.
- Juhendi lõppu koristamise või lähtestamise samm, kui praktikum käivitab teenuseid või tekitab andmeid.

## Iga sammu juures kontrolli

- Kas on selge, kus see samm tehakse.
- Kas õppija saab aru, mida käsk või koodiplokk teeb.
- Kas juhendis on öeldud, milline tulemus kinnitab, et samm õnnestus.
- Kas võimalik komistuskoht on ennetatud kohe samas kohas, mitte alles lõpus.

## Levinud vead ja tõrkeotsing

- Kasuta formaati: sümptom, tõenäoline põhjus, lahendus.
- Kirjelda veateadet nii, nagu õppija seda näeb, ja selgita, mida see tähendab.
- Ära eelda, et õppija oskab veateadet iseseisvalt tõlgendada.
- Selles repos tasub eriti ennetada segadust teemadel: vale kaust, host vs konteiner, shell vs `psql`, teenus ei käivitu, port on hõivatud, `.env` fail puudub.

## Mida vältida

- Ära kuhja ühte juhendisse liiga palju uusi mõisteid.
- Ära anna ainult käskude loendit ilma selgituse ja oodatava tulemuseta.
- Ära kopeeri dokumentatsiooni stiili. Õppejuhend peab olema õpetav.
- Ära kirjuta materjali nii, nagu see oleks mõeldud tehnilise taustaga osalejale.
- Ära jäta olulisi eeldusi nimetamata.
- Ära kasuta seletamata lühendeid.
- Ära paku algajale korraga mitut võrdselt sobivat tööteed ilma soovituseta, millest alustada.
