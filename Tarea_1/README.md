# Ejercicio 5 — Análisis del Mercado Laboral Mexicano con ENOE (2022–2025)

> Tarea 1 · Macroeconomía II · Maestría en Economía · El Colegio de México

---

## Descripción

Este repositorio contiene el análisis del mercado laboral mexicano para el periodo 2022–2025 utilizando los microdatos de la **Encuesta Nacional de Ocupación y Empleo Nueva Edición (ENOE)** publicada por el INEGI. El ejercicio calcula nueve indicadores trimestrales del mercado de trabajo, desde tasas de desempleo hasta transiciones laborales mediante un panel rotativo.

El script principal está escrito en **R**, con un bloque en **Julia** para el procesamiento eficiente del panel rotativo (inciso 5i). Esta arquitectura híbrida reduce el uso de memoria de ~2.3 GB a ~90 MB y el tiempo de ejecución de más de una hora a 5–10 minutos, al procesar 15 trimestres consecutivos de microdatos.

---

## Indicadores calculados

| Inciso | Indicador | Metodología |
|--------|-----------|-------------|
| **5b** | Tasa de desempleo trimestral | `desocupados / PEA × 100`, ponderado por `fac_tri` |
| **5c** | Tasa de subocupación trimestral | `subocupados / ocupados × 100`, ponderado por `fac_tri` |
| **5d** | PNEA disponible para trabajar | `PNEA disponible / PNEA total × 100` |
| **5e** | Distribución del tamaño de empresa | Clasificación por `ambito2`: micro, pequeña, mediana, grande |
| **5f** | Ocupados buscando otro empleo | `buscando otro / ocupados × 100` |
| **5g** | Ingreso promedio por grupo de edad | Promedio ponderado `weighted.mean(ingocup, fac_tri)` |
| **5h** | Distribución de ocupados por sexo | Participación porcentual hombres/mujeres |
| **5i** | Transiciones empleo ↔ desempleo | Panel rotativo: fracciones E→U y U→E por trimestre |

---

## Arquitectura del código

El análisis se implementa con una arquitectura híbrida R–Julia:

- **R** es el lenguaje principal. Maneja la carga y limpieza de datos, los cálculos de todos los indicadores y la generación de gráficas y tablas.
- **Julia** se utiliza exclusivamente en el inciso 5i para construir el panel longitudinal y calcular la matriz de transiciones. El procesamiento de grandes volúmenes de datos por individuo–trimestre es notablemente más eficiente en Julia que en R puro.

La comunicación entre ambos lenguajes se realiza mediante el paquete `JuliaCall`, que permite ejecutar funciones de Julia directamente desde una sesión de R sin archivos intermedios.

---

## Requisitos

### Lenguajes

- **R** ≥ 4.0.0 — [https://cran.r-project.org](https://cran.r-project.org)
- **Julia** ≥ 1.6.0 — [https://julialang.org/downloads](https://julialang.org/downloads)

### Paquetes de R

```r
install.packages(c("JuliaCall", "tidyverse"))
```

| Paquete | Uso |
|---------|-----|
| `tidyverse` | Manipulación de datos, gráficas (`dplyr`, `ggplot2`, `readr`, `stringr`) |
| `JuliaCall` | Interfaz R–Julia para el panel rotativo |

### Paquetes de Julia

Únicamente `DataFrames`, que viene **incluido en la distribución estándar de Julia ≥ 1.6**; no requiere instalación adicional.

---

## Datos

Los microdatos de la ENOE Nueva Edición se descargan del sitio oficial del INEGI:

🔗 [https://www.inegi.org.mx/programas/enoe/15ymas/](https://www.inegi.org.mx/programas/enoe/15ymas/)

El script espera la siguiente estructura de carpetas:

```
RUTA_BASE/
├── enoe_n_2022_trim1_csv/
│   ├── SDEMT202201.csv
│   └── COE1T202201.csv
├── enoe_n_2022_trim2_csv/
│   ├── SDEMT202202.csv
│   └── COE1T202202.csv
│   ...
└── enoe_n_2025_trim3_csv/
    ├── SDEMT202503.csv
    └── COE1T202503.csv
```

**Archivos utilizados por trimestre:**

- `SDEMT` — Cuestionario sociodemográfico: identificación de individuo, clasificación laboral (`clase2`), ponderador (`fac_tri`), edad y sexo.
- `COE1T` — Cuestionario de Ocupación y Empleo I: ingresos (`ingocup`), tamaño de empresa (`ambito2`), subocupación (`sub_o`), búsqueda de empleo (`busqueda`).

> El archivo `COE2T` no se utiliza en este ejercicio.

---

## Uso

### 1. Clonar el repositorio

```bash
git clone https://github.com/usuario/ejercicio5-enoe.git
cd ejercicio5-enoe
```

### 2. Configurar la ruta de datos

Abrir el script y ajustar la variable `RUTA_BASE` en la sección de configuración:

```r
RUTA_BASE <- "C:/ruta/a/tus/datos/ENOE"
```

### 3. Ejecutar el análisis

```r
source("ejercicio5_ENOEN_2022_2025_OPTIMIZADO.R")
```

El script procesa los 15 trimestres de forma secuencial, imprime el progreso en consola y guarda los resultados en `RUTA_BASE/Resultados Optim/`.

---

## Archivos del repositorio

```
.
├── ejercicio5_ENOEN_2022_2025_OPTIMIZADO.R   # Script principal
├── EJERCICIO5_ENOE_DOCUMENTACION_COMPLETA.Rmd # Documentación completa en RMarkdown
└── README.md
```

### Resultados generados al ejecutar el script

```
Resultados Optim/
├── resultados_5b_desempleo.csv
├── resultados_5c_subempleo.csv
├── resultados_5d_trabajadores_disponibles.csv
├── resultados_5e_tamano_empresa.csv
├── resultados_5e_empresas_pmg.csv
├── resultados_5f_buscando_empleo.csv
├── resultados_5g_ingreso_edad_positivo.csv
├── resultados_5g_ingreso_edad0.csv
├── resultados_5i_transiciones_EU_UE.csv
├── grafica_5g_ingreso_edad_positivo.png
└── grafica_5h_distribucion_sexo.png
```

---

## Notas metodológicas

### Uso obligatorio del ponderador `fac_tri`

La ENOE es una encuesta con diseño muestral complejo; cada observación no representa a una persona, sino a un conjunto de personas en la población. Todos los cálculos utilizan el factor de expansión trimestral `fac_tri` para obtener estimaciones representativas a nivel poblacional.

```r
# Incorrecto: tasa de la muestra
sum(clase2 == "2") / sum(clase2 %in% c("1","2"))

# Correcto: estimación poblacional
sum(fac_tri[clase2 == "2"]) / sum(fac_tri[clase2 %in% c("1","2")])
```

### Filtros de calidad (INEGI)

Antes del análisis se aplican los filtros recomendados por el INEGI:

```r
r_def %in% c("0", "00")   # Entrevista completa
c_res %in% c("1", "3")    # Residente habitual o ausente temporal
eda >= 15                  # Población en edad de trabajar
```

### Panel rotativo (inciso 5i)

Las tasas de transición E→U y U→E son **condicionales a la re-entrevista**, es decir, se calculan únicamente para los individuos que aparecen en dos trimestres consecutivos. El identificador de individuo (`id_panel`) se construye concatenando once variables de identificación de vivienda, hogar y persona. La resolución de duplicados dentro de un mismo trimestre se realiza eligiendo la observación con el mayor valor de `mes_cal`.

---

## Optimización de memoria

| Estrategia | Descripción |
|------------|-------------|
| Carga selectiva de columnas | Se leen solo 20 de ~200 variables disponibles por archivo |
| Filtrado previo al merge | Se eliminan registros inválidos antes de combinar SDEMT y COE1T |
| Liberación explícita de memoria | `rm()` + `gc()` después de cada paso intermedio |
| Procesamiento en Julia | Agrupación y deduplicación de millones de filas por individuo–trimestre |

---

## Referencia

INEGI (2022–2025). *Encuesta Nacional de Ocupación y Empleo (ENOE) Nueva Edición*. Instituto Nacional de Estadística y Geografía. [https://www.inegi.org.mx/programas/enoe/15ymas/](https://www.inegi.org.mx/programas/enoe/15ymas/)
