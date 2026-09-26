# spark-jev

## Français

UDF SQL PySpark pour décision sémantique structurée.

Distribuez les deux fichiers aux exécuteurs, enregistrez `jev_if`, puis interrogez `jev_if(review).route`. Prévoyez un petit ensemble de candidats et les réexécutions Spark.

## English

PySpark SQL UDF for structured semantic decisions.

Install PySpark on driver and executors, distribute both Python files with `spark.sparkContext.addPyFile("spark_jev.py")` and `addPyFile("jev_common.py")`, then `register_jev_if(spark, "Does this review recommend the movie?")`. Query `SELECT jev_if(review).route FROM reviews`. Put `JEV_API_KEY` on each executor. The remote call is per UDF evaluation, so use bounded candidate sets and account for Spark retries.

## Español

UDF SQL de PySpark para decisiones semánticas estructuradas.

Distribuya los dos archivos a los ejecutores, registre `jev_if` y consulte `jev_if(review).route`. Limite los candidatos y tenga en cuenta los reintentos de Spark.

## Contract / Contrat / Contrato

`yes`, `no`, `review`, `failure`; threshold default `0.8`. `review` is a real undecided state. Empty or oversized input becomes `review`; transport or invalid-response errors become `failure`. The shared client caps input at 32 KiB, response at 100 KiB, timeout at 10 s and calls at 10,000 per process; YAML templates enforce their own input and response bounds. No raw input is logged by this project. User data goes to the TypeSafe Jev API.

FR : `review` exige une revue humaine ; `failure` signale une erreur. Le contenu est envoyé à l’API TypeSafe Jev.

ES: `review` requiere revisión humana; `failure` indica un error. El contenido se envía a la API TypeSafe Jev.

## TLS / TLS / TLS

FR : si votre installation Python ne trouve pas les certificats racines, définissez `SSL_CERT_FILE` vers un bundle CA valide (par exemple `certifi.where()`). Ne désactivez pas la vérification TLS.

EN: if Python cannot find root certificates, set `SSL_CERT_FILE` to a valid CA bundle (for example `certifi.where()`). Keep TLS verification enabled.

ES: si Python no encuentra los certificados raíz, defina `SSL_CERT_FILE` con un paquete CA válido (por ejemplo `certifi.where()`). Mantenga activa la verificación TLS.

## Development / Développement / Desarrollo

`python -m unittest discover -p "test_*.py" -v`

Platform / Plateforme / Plataforma: [Apache Spark documentation](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html).

MIT license. Community project; not an official Apache Spark integration.
