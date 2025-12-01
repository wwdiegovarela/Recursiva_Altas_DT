import pandas as pd
import unicodedata
from typing import Dict, List, Optional


def normalize_text(value: Optional[str]) -> Optional[str]:
    if pd.isna(value):
        return value
    text = str(value).strip()
    text = unicodedata.normalize('NFD', text)
    text = ''.join(ch for ch in text if unicodedata.category(ch) != 'Mn')
    return text


def apply_mapping_to_series(
    series: pd.Series,
    mapping: Dict[str, str],
    *,
    normalize: bool = True,
    on_miss: str = "keep",  # keep | none | error
) -> pd.Series:
    if normalize:
        normalized_keys = {normalize_text(k): v for k, v in mapping.items()}
        def map_func(x):
            key = normalize_text(x)
            if key in normalized_keys:
                return normalized_keys[key]
            if on_miss == "keep":
                return x
            if on_miss == "none":
                return None
            raise KeyError(f"Valor sin mapeo: {x}")
        return series.apply(map_func)
    else:
        def map_func_raw(x):
            if x in mapping:
                return mapping[x]
            if on_miss == "keep":
                return x
            if on_miss == "none":
                return None
            raise KeyError(f"Valor sin mapeo: {x}")
        return series.apply(map_func_raw)


def apply_mappings_to_df(
    df: pd.DataFrame,
    mappings: List[Dict],
    logs: Optional[List[str]] = None,
) -> pd.DataFrame:
    if not mappings:
        return df
    result_df = df.copy()
    for rule in mappings:
        target_field = rule.get("campo_objetivo_en_data")
        mapping_dict = rule.get("tabla") or {}
        normalize = rule.get("normalizacion", True)
        on_miss = rule.get("comportamiento_si_no_hay_match", "keep")
        in_place = rule.get("in_place", True)
        output_field = rule.get("nuevo_campo")

        if target_field not in result_df.columns:
            if logs is not None:
                logs.append(f"Campo a mapear no encontrado: {target_field}")
            continue

        mapped_series = apply_mapping_to_series(
            result_df[target_field], mapping_dict, normalize=normalize, on_miss=on_miss
        )

        if in_place or not output_field:
            result_df[target_field] = mapped_series
        else:
            result_df[output_field] = mapped_series

        if logs is not None:
            logs.append(f"Diccionario aplicado sobre {target_field}")

    return result_df


# Estructura de ejemplo: pega tus reglas aquí desde la conversación
MAPPINGS_ALTAS: List[Dict] = []

# Diccionario de comunas por código numérico
MAPPING_COMUNAS_COD_TO_NOMBRE: Dict[str, str] = {
    1: "IQUIQUE",
    2: "ALTO HOSPICIO",
    3: "CAMIÑA",
    4: "PICA",
    5: "POZO ALMONTE",
    6: "HUARA",
    7: "COLCHANE",
    8: "MEJILLONES",
    9: "ANTOFAGASTA",
    10: "TALTAL",
    11: "SIERRA GORDA",
    12: "CALAMA",
    13: "SAN PEDRO DE ATACAMA",
    14: "OLLAGÜE",
    15: "TOCOPILLA",
    16: "MARÍA ELENA",
    17: "COPIAPÓ",
    18: "CALDERA",
    19: "TIERRA AMARILLA",
    20: "DIEGO DE ALMAGRO",
    21: "CHAÑARAL",
    22: "HUASCO",
    23: "FREIRINA",
    24: "VALLENAR",
    25: "ALTO DEL CARMEN",
    26: "COQUIMBO",
    27: "LA SERENA",
    28: "ANDACOLLO",
    29: "PAIGUANO",
    30: "VICUÑA",
    31: "LA HIGUERA",
    32: "CANELA",
    33: "ILLAPEL",
    34: "SALAMANCA",
    35: "LOS VILOS",
    36: "COMBARBALÁ",
    37: "MONTE PATRIA",
    38: "PUNITAQUI",
    39: "OVALLE",
    40: "RÍO HURTADO",
    41: "JUAN FERNÁNDEZ",
    42: "VALPARAÍSO",
    43: "CASABLANCA",
    44: "CONCÓN",
    45: "VIÑA DEL MAR",
    46: "PUCHUNCAVÍ",
    47: "QUINTERO",
    48: "ISLA DE PASCUA",
    49: "CALLE LARGA",
    50: "LOS ANDES",
    51: "SAN ESTEBAN",
    52: "RINCONADA",
    53: "CABILDO",
    54: "PETORCA",
    55: "ZAPALLAR",
    56: "LA LIGUA",
    57: "PAPUDO",
    58: "HIJUELAS",
    59: "QUILLOTA",
    60: "CALERA",
    61: "NOGALES",
    62: "LA CRUZ",
    63: "CARTAGENA",
    64: "ALGARROBO",
    65: "SAN ANTONIO",
    66: "EL TABO",
    67: "SANTO DOMINGO",
    68: "EL QUISCO",
    69: "LLAILLAY",
    70: "CATEMU",
    71: "SAN FELIPE",
    72: "PUTAENDO",
    73: "SANTA MARÍA",
    74: "PANQUEHUE",
    75: "VILLA ALEMANA",
    76: "OLMUÉ",
    77: "LIMACHE",
    78: "QUILPUÉ",
    79: "QUINTA DE TILCOCO",
    80: "RENGO",
    81: "REQUÍNOA",
    82: "SAN VICENTE",
    83: "PICHIDEGUA",
    84: "PEUMO",
    85: "OLIVAR",
    86: "MOSTAZAL",
    87: "MALLOA",
    88: "MACHALÍ",
    89: "CODEGUA",
    90: "GRANEROS",
    91: "DOÑIHUE",
    92: "COLTAUCO",
    93: "COINCO",
    94: "RANCAGUA",
    95: "LAS CABRAS",
    96: "PAREDONES",
    97: "NAVIDAD",
    98: "LITUECHE",
    99: "LA ESTRELLA",
    100: "MARCHIHUE",
    101: "PICHILEMU",
    102: "LOLOL",
    103: "SANTA CRUZ",
    104: "PUMANQUE",
    105: "PLACILLA",
    106: "PERALILLO",
    107: "SAN FERNANDO",
    108: "CHÉPICA",
    109: "CHIMBARONGO",
    110: "NANCAGUA",
    111: "PALMILLA",
    112: "PELARCO",
    113: "RÍO CLARO",
    114: "MAULE",
    115: "PENCAHUE",
    116: "SAN RAFAEL",
    117: "EMPEDRADO",
    118: "SAN CLEMENTE",
    119: "CONSTITUCIÓN",
    120: "TALCA",
    121: "CUREPTO",
    122: "PELLUHUE",
    123: "CHANCO",
    124: "CAUQUENES",
    125: "ROMERAL",
    126: "SAGRADA FAMILIA",
    127: "TENO",
    128: "VICHUQUÉN",
    129: "RAUCO",
    130: "MOLINA",
    131: "CURICÓ",
    132: "HUALAÑÉ",
    133: "LICANTÉN",
    134: "PARRAL",
    135: "RETIRO",
    136: "SAN JAVIER",
    137: "VILLA ALEGRE",
    138: "YERBAS BUENAS",
    139: "LONGAVÍ",
    140: "LINARES",
    141: "COLBÚN",
    142: "FLORIDA",
    143: "LOTA",
    144: "HUALQUI",
    145: "CHIGUAYANTE",
    146: "PENCO",
    147: "CORONEL",
    148: "SANTA JUANA",
    149: "TALCAHUANO",
    150: "TOMÉ",
    151: "HUALPÉN",
    152: "CONCEPCIÓN",
    153: "SAN PEDRO DE LA PAZ",
    154: "CAÑETE",
    155: "CURANILAHUE",
    156: "CONTULMO",
    157: "LOS ÁLAMOS",
    158: "ARAUCO",
    159: "TIRÚA",
    160: "LEBU",
    161: "NEGRETE",
    162: "TUCAPEL",
    163: "YUMBEL",
    164: "ALTO BIOBÍO",
    165: "SANTA BÁRBARA",
    166: "SAN ROSENDO",
    167: "QUILACO",
    168: "NACIMIENTO",
    169: "QUILLECO",
    170: "LAJA",
    171: "CABRERO",
    172: "ANTUCO",
    173: "LOS ÁNGELES",
    174: "MULCHÉN",
    175: "PORTEZUELO",
    176: "QUILLÓN",
    177: "QUIRIHUE",
    178: "RÁNQUIL",
    179: "SAN CARLOS",
    180: "SAN IGNACIO",
    181: "PINTO",
    182: "SAN NICOLÁS",
    183: "TREGUACO",
    184: "YUNGAY",
    185: "SAN FABIÁN",
    186: "ÑIQUÉN",
    187: "PEMUCO",
    188: "BULNES",
    189: "EL CARMEN",
    190: "CHILLÁN VIEJO",
    191: "COIHUECO",
    192: "COELEMU",
    193: "COBQUECURA",
    194: "CHILLÁN",
    195: "NINHUE",
    196: "PERQUENCO",
    197: "NUEVA IMPERIAL",
    198: "PITRUFQUÉN",
    199: "PADRE LAS CASAS",
    200: "PUCÓN",
    201: "VILCÚN",
    202: "TEODORO SCHMIDT",
    203: "CHOLCHOL",
    204: "VILLARRICA",
    205: "TOLTÉN",
    206: "SAAVEDRA",
    207: "MELIPEUCO",
    208: "CUNCO",
    209: "LAUTARO",
    210: "LONCOCHE",
    211: "CARAHUE",
    212: "CURARREHUE",
    213: "TEMUCO",
    214: "GALVARINO",
    215: "GORBEA",
    216: "FREIRE",
    217: "VICTORIA",
    218: "TRAIGUÉN",
    219: "RENAICO",
    220: "PURÉN",
    221: "LUMACO",
    222: "LOS SAUCES",
    223: "LONQUIMAY",
    224: "COLLIPULLI",
    225: "CURACAUTÍN",
    226: "ANGOL",
    227: "ERCILLA",
    228: "PUERTO VARAS",
    229: "MAULLÍN",
    230: "LLANQUIHUE",
    231: "FRUTILLAR",
    232: "CALBUCO",
    233: "FRESIA",
    234: "COCHAMÓ",
    235: "PUERTO MONTT",
    236: "LOS MUERMOS",
    237: "QUEILÉN",
    238: "ANCUD",
    239: "CHONCHI",
    240: "QUINCHAO",
    241: "QUEMCHI",
    242: "QUELLÓN",
    243: "CASTRO",
    244: "PUQUELDÓN",
    245: "DALCAHUE",
    246: "CURACO DE VÉLEZ",
    247: "SAN JUAN DE LA COSTA",
    248: "PUERTO OCTAY",
    249: "OSORNO",
    250: "RÍO NEGRO",
    251: "PUYEHUE",
    252: "SAN PABLO",
    253: "PURRANQUE",
    254: "HUALAIHUÉ",
    255: "PALENA",
    256: "CHAITÉN",
    257: "FUTALEUFÚ",
    258: "COYHAIQUE",
    259: "LAGO VERDE",
    260: "AYSÉN",
    261: "GUAITECAS",
    262: "CISNES",
    263: "COCHRANE",
    264: "O´HIGGINS",
    265: "TORTEL",
    266: "RÍO IBÁÑEZ",
    267: "CHILE CHICO",
    268: "SAN GREGORIO",
    269: "RÍO VERDE",
    270: "LAGUNA BLANCA",
    271: "PUNTA ARENAS",
    272: "ANTÁRTICA",
    273: "CABO DE HORNOS",
    274: "PORVENIR",
    275: "PRIMAVERA",
    276: "TIMAUKEL",
    277: "NATALES",
    278: "TORRES DEL PAINE",
    279: "ESTACIÓN CENTRAL",
    280: "LAS CONDES",
    281: "LA REINA",
    282: "LA PINTANA",
    283: "LA GRANJA",
    284: "LA FLORIDA",
    285: "LA CISTERNA",
    286: "SANTIAGO",
    287: "CERRILLOS",
    288: "INDEPENDENCIA",
    289: "CONCHALÍ",
    290: "EL BOSQUE",
    291: "HUECHURABA",
    292: "CERRO NAVIA",
    293: "MAIPÚ",
    294: "VITACURA",
    295: "ÑUÑOA",
    296: "PEDRO AGUIRRE CERDA",
    297: "PEÑALOLÉN",
    298: "PROVIDENCIA",
    299: "LO BARNECHEA",
    300: "PUDAHUEL",
    301: "MACUL",
    302: "QUILICURA",
    303: "RECOLETA",
    304: "RENCA",
    305: "SAN JOAQUÍN",
    306: "SAN MIGUEL",
    307: "SAN RAMÓN",
    308: "LO PRADO",
    309: "QUINTA NORMAL",
    310: "LO ESPEJO",
    311: "PIRQUE",
    312: "PUENTE ALTO",
    313: "SAN JOSÉ DE MAIPO",
    314: "TILTIL",
    315: "LAMPA",
    316: "COLINA",
    317: "CALERA DE TANGO",
    318: "BUIN",
    319: "SAN BERNARDO",
    320: "PAINE",
    321: "MELIPILLA",
    322: "CURACAVÍ",
    323: "MARÍA PINTO",
    324: "ALHUÉ",
    325: "SAN PEDRO",
    326: "ISLA DE MAIPO",
    327: "EL MONTE",
    328: "TALAGANTE",
    329: "PEÑAFLOR",
    330: "PADRE HURTADO",
    331: "LANCO",
    332: "VALDIVIA",
    333: "PANGUIPULLI",
    334: "PAILLACO",
    335: "MARIQUINA",
    336: "MÁFIL",
    337: "LOS LAGOS",
    338: "CORRAL",
    339: "LAGO RANCO",
    340: "LA UNIÓN",
    341: "FUTRONO",
    342: "RÍO BUENO",
    343: "CAMARONES",
    344: "ARICA",
    345: "GENERAL LAGOS",
    346: "PUTRE",
}

# Diccionario: REM_PERIODO_PAGO (código -> glosa)
MAPPING_REM_PERIODO_PAGO: Dict[str, str] = {
    1: "Mensual",
    2: "Bimestral",
    3: "Trimestral",
    4: "Cuatrimestral",
    5: "Semestral",
    6: "Anual",
}

# Diccionario: REM_FORMA_PAGO (código -> glosa)
MAPPING_REM_FORMA_PAGO: Dict[str, str] = {
    1: "Dinero en efectivo",
    2: "Cheque",
    3: "Vale vista",
    4: "Depósito bancario",
    5: "Transferencia bancaria",
}

# Diccionario: GRAT_FORMA_PAGO (código -> glosa)
MAPPING_GRAT_FORMA_PAGO: Dict[str, str] = {
    1: "No pactado en contrato de trabajo",
    2: "Artículo 47 Código de Trabajo",
    3: "Modalidad convencional superior al mínimo legal",
    4: "Artículo 50 del Código del Trabajo",
    5: "Sin obligación legal de pago",
}

# Diccionario: REM_AFP (código -> glosa)
MAPPING_REM_AFP: Dict[str, str] = {
    6: "AFP PROVIDA",
    11: "AFP PLANVITAL",
    13: "AFP CUPRUM",
    14: "AFP HABITAT",
    19: "UNO S.A.",
    27: "AFP BANSANDER",
    31: "AFP CAPITAL",
    100: "NO AFILIADO",
    101: "IPS",
    103: "AFP MODELO",
    104: "PERSONA EXCEPTUADA DE COTIZAR",
}

# Diccionario: REM_ANTICIPO (código -> glosa)
MAPPING_REM_ANTICIPO: Dict[str, str] = {
    1: "Sin anticipo",
    2: "Por hora",
    3: "Diario",
    4: "Semanal",
    5: "Quincenal",
}

# Diccionario: REM_SALUD (código -> glosa)
MAPPING_REM_SALUD: Dict[str, str] = {
    1: "ISAPRE CRUZ BLANCA",
    3: "ISAPRE BANMEDICA",
    4: "ISAPRE COLMENA",
    9: "ISAPRE CONSALUD",
    12: "ISAPRE VIDA TRES",
    37: "ISAPRE CHUQUICAMATA",
    38: "ISAPRE CRUZ DEL NORTE",
    39: "ISAPRE FUSAT",
    40: "ISAPRE FUNDACION",
    41: "ISAPRE RIO BLANCO",
    42: "ISAPRE SAN LORENZO",
    43: "ISAPRE NUEVA MASVIDA",
    102: "FONASA",
}

# Diccionario: TIPO_JORNADA (código -> glosa)
MAPPING_TIPO_JORNADA: Dict[str, str] = {
    1: "Jornada Semanal Ordinaria",
    2: "Jornada Semanal Extendida",
    3: "Jornada Bisemanal",
    4: "Jornada Mensual",
    5: "Jornada Diaria",
    6: "Jornada Excepcional",
    7: "Jornada Art. 22",
    8: "Art. 38 Semanal Ordinaria",
    9: "Art. 38 Semanal Extendida",
}

# Diccionario: TURNOS (código -> glosa)
MAPPING_TURNOS: Dict[str, str] = {
    1: "HORARIO FIJO SIN TURNO",
    2: "MAÑANA",
    3: "TARDE",
    4: "NOCHE",
}

# Diccionario: TIPO_CONTRATO (código -> glosa)
MAPPING_TIPO_CONTRATO: Dict[str, str] = {
    1: "Indefinido",
    2: "Plazo fijo",
    3: "Obra o faena",
}

# Diccionario: CATEGORIA_CONTRATO (código -> glosa)
MAPPING_CATEGORIA_CONTRATO: Dict[str, str] = {
    1: "Contrato de Trabajo sujeto a reglas generales",
}

# Diccionario: AFECTO_A (código -> glosa)
MAPPING_AFECTO_A: Dict[str, str] = {
    1: "Afecto a contrato colectivo",
    2: "Afecto a convenio colectivo",
    3: "Afecto a acuerdo de grupo negociador",
    4: "Afecto a fallo arbitral",
    5: "Afecto a extensión de beneficio",
}

# Reglas para aplicar el diccionario de comunas a los tres campos
MAPPINGS_ALTAS.extend([
    {
        "nombre_diccionario": "comunas_por_codigo",
        "tabla": MAPPING_COMUNAS_COD_TO_NOMBRE,
        "campo_objetivo_en_data": "comuna",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "comunas_por_codigo",
        "tabla": MAPPING_COMUNAS_COD_TO_NOMBRE,
        "campo_objetivo_en_data": "comuna_celebracion",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "comunas_por_codigo",
        "tabla": MAPPING_COMUNAS_COD_TO_NOMBRE,
        "campo_objetivo_en_data": "faena_comuna",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "rem_periodo_pago_por_codigo",
        "tabla": MAPPING_REM_PERIODO_PAGO,
        "campo_objetivo_en_data": "rem_periodo_pago",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "rem_forma_pago_por_codigo",
        "tabla": MAPPING_REM_FORMA_PAGO,
        "campo_objetivo_en_data": "rem_forma_pago",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "grat_forma_pago_por_codigo",
        "tabla": MAPPING_GRAT_FORMA_PAGO,
        "campo_objetivo_en_data": "grat_forma_pago",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "rem_afp_por_codigo",
        "tabla": MAPPING_REM_AFP,
        "campo_objetivo_en_data": "rem_afp",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "rem_anticipo_por_codigo",
        "tabla": MAPPING_REM_ANTICIPO,
        "campo_objetivo_en_data": "rem_anticipo",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "rem_salud_por_codigo",
        "tabla": MAPPING_REM_SALUD,
        "campo_objetivo_en_data": "rem_salud",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "tipo_jornada_por_codigo",
        "tabla": MAPPING_TIPO_JORNADA,
        "campo_objetivo_en_data": "tipo_jornada",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "turnos_por_codigo",
        "tabla": MAPPING_TURNOS,
        "campo_objetivo_en_data": "turnos",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "tipo_contrato_por_codigo",
        "tabla": MAPPING_TIPO_CONTRATO,
        "campo_objetivo_en_data": "tipo_contrato",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "categoria_contrato_por_codigo",
        "tabla": MAPPING_CATEGORIA_CONTRATO,
        "campo_objetivo_en_data": "categoria_contrato",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
    {
        "nombre_diccionario": "afecto_a_por_codigo",
        "tabla": MAPPING_AFECTO_A,
        "campo_objetivo_en_data": "afecto_a",
        "comportamiento_si_no_hay_match": "keep",
        "normalizacion": True,
        "in_place": True,
    },
])


