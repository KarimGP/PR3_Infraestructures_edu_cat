"""Cataleg de tipus d'incidencia.

Els pesos relatius i les probabilitats d'urgencia venen de l'experiencia
operativa en manteniment d'infraestructures educatives, no d'una font
publicada. Estan documentats com a suposits al README.

Distribucio per families acordada:
  Fontaneria 27% | Climatitzacio 27% | Paleteria  16%
  Electricitat 15% | Fusteria    11% | Estructura  4%

prob_seguretat / prob_interrupcio: probabilitat que una incidencia
d'aquest tipus dispari el criteri d'urgencia (SLA de 24h). No son fixes
perque una mateixa averia pot interrompre l'activitat o no segons on i
quan passi.
"""
import os
import psycopg2
from psycopg2.extras import execute_values

# (codi, familia, descripcio, prob_seguretat, prob_interrupcio, pes, cost_mitja)
TIPUS = [
    # FONTANERIA - 27%
    ("FON-001", "FONTANERIA", "Fuita d'aigua en canonada",           0.05, 0.35, 0.0700,  320),
    ("FON-002", "FONTANERIA", "Wc o lavabo obstruit",                0.00, 0.20, 0.0800,  140),
    ("FON-003", "FONTANERIA", "Aixeta o cisterna avariada",          0.00, 0.05, 0.0650,   95),
    ("FON-004", "FONTANERIA", "Manca de subministrament d'aigua",    0.10, 0.90, 0.0250,  480),
    ("FON-005", "FONTANERIA", "Desguas embussat",                    0.05, 0.30, 0.0300,  210),
    # CLIMATITZACIO - 27%
    ("CLI-001", "CLIMATITZACIO", "Caldera aturada",                  0.05, 0.75, 0.0550,  850),
    ("CLI-002", "CLIMATITZACIO", "Radiador sense escalfar",          0.00, 0.15, 0.0700,  180),
    ("CLI-003", "CLIMATITZACIO", "Aire condicionat avariat",         0.00, 0.30, 0.0650,  420),
    ("CLI-004", "CLIMATITZACIO", "Fuita al circuit de calefaccio",   0.10, 0.45, 0.0400,  620),
    ("CLI-005", "CLIMATITZACIO", "Termostat o regulacio KO",         0.00, 0.10, 0.0400,  160),
    # PALETERIA - 16%
    ("PAL-001", "PALETERIA", "Despreniment d'arrebossat o guix",     0.45, 0.25, 0.0350,  380),
    ("PAL-002", "PALETERIA", "Humitats o filtracions",               0.05, 0.15, 0.0550,  520),
    ("PAL-003", "PALETERIA", "Paviment aixecat o trencat",           0.40, 0.10, 0.0400,  290),
    ("PAL-004", "PALETERIA", "Rajola o enrajolat despres",           0.25, 0.05, 0.0300,  175),
    # ELECTRICITAT - 15%
    ("ELE-001", "ELECTRICITAT", "Tall de subministrament electric",  0.35, 0.95, 0.0250,  540),
    ("ELE-002", "ELECTRICITAT", "Enllumenat fos en aula o passadis", 0.05, 0.15, 0.0500,   85),
    ("ELE-003", "ELECTRICITAT", "Quadre electric amb avaria",        0.55, 0.60, 0.0250,  690),
    ("ELE-004", "ELECTRICITAT", "Endoll o interruptor trencat",      0.20, 0.05, 0.0300,  110),
    ("ELE-005", "ELECTRICITAT", "Enllumenat d'emergencia KO",        0.70, 0.05, 0.0200,  230),
    # FUSTERIA - 11%
    ("FUS-001", "FUSTERIA", "Porta que no tanca o no obre",          0.15, 0.20, 0.0350,  165),
    ("FUS-002", "FUSTERIA", "Finestra trencada o encallada",         0.30, 0.10, 0.0300,  240),
    ("FUS-003", "FUSTERIA", "Persiana avariada",                     0.05, 0.05, 0.0250,  195),
    ("FUS-004", "FUSTERIA", "Mobiliari fix malmes",                  0.10, 0.05, 0.0200,  130),
    # ESTRUCTURA - 4%
    ("EST-001", "ESTRUCTURA", "Esquerda en element estructural",     0.75, 0.35, 0.0150, 1800),
    ("EST-002", "ESTRUCTURA", "Filtracio en coberta",                0.15, 0.25, 0.0150, 1350),
    ("EST-003", "ESTRUCTURA", "Deteriorament de facana",             0.60, 0.10, 0.0100, 2400),
]


def main():
    total = sum(t[5] for t in TIPUS)
    print(f"Tipus definits: {len(TIPUS)}")
    print(f"Suma de pesos : {total:.4f}")
    if abs(total - 1.0) > 0.0001:
        raise SystemExit(f"ERROR: els pesos han de sumar 1.0, sumen {total}")

    conn = psycopg2.connect(
        host="localhost", port=5432,
        dbname=os.getenv("POSTGRES_DB", "infraedu"),
        user=os.getenv("POSTGRES_USER", "pr3"),
        password=os.getenv("POSTGRES_PASSWORD", "pr3_local_dev"),
    )
    with conn, conn.cursor() as cur:
        cur.execute("TRUNCATE ops.tipus_incidencia CASCADE")
        execute_values(cur,
            "INSERT INTO ops.tipus_incidencia (codi_tipus, familia, descripcio, "
            "prob_seguretat, prob_interrupcio, pes_relatiu, cost_mitja) VALUES %s",
            TIPUS)
    conn.close()

    print("\nDistribucio per familia:")
    fam = {}
    for t in TIPUS:
        fam[t[1]] = fam.get(t[1], 0) + t[5]
    for f, p in sorted(fam.items(), key=lambda x: -x[1]):
        print(f"  {f:<15} {p*100:5.1f}%")


if __name__ == "__main__":
    main()
