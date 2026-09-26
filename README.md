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

FR : le seuil par défaut est `0.8`. Les routes sont `yes`, `no`, `review` et `failure`. Une entrée vide ou supérieure à 32 Kio donne `review` ; une erreur de transport ou de réponse donne `failure`. Le client limite les appels à 10 000 par processus, à 10 s par appel et à 100 Kio par réponse. Un cache LRU conserve au plus 1 024 verdicts valides par empreinte SHA-256 ; il ne conserve pas le texte brut. Les données sont envoyées à TypeSafe Jev.

EN: the default threshold is `0.8`. Routes are `yes`, `no`, `review`, and `failure`. Empty input or input over 32 KiB becomes `review`; transport or response errors become `failure`. The client caps calls at 10,000 per process, 10 seconds per call, and 100 KiB per response. An LRU cache keeps at most 1,024 valid verdicts by SHA-256 digest; it does not store raw text. Data is sent to TypeSafe Jev.

ES: el umbral predeterminado es `0.8`. Las rutas son `yes`, `no`, `review` y `failure`. Una entrada vacía o superior a 32 KiB produce `review`; los errores de transporte o respuesta producen `failure`. El cliente limita las llamadas a 10 000 por proceso, a 10 s por llamada y a 100 KiB por respuesta. Una caché LRU conserva como máximo 1 024 decisiones válidas por huella SHA-256; no almacena el texto original. Los datos se envían a TypeSafe Jev.

## SQL deployment / Déploiement SQL / Despliegue SQL

FR : distribuez `spark_jev.py` et `jev_common.py` aux exécuteurs, puis enregistrez la fonction. Elle est déclarée non déterministe pour éviter une optimisation SQL incorrecte. Limitez les lignes candidates avant l’appel distant et définissez la clé Jev dans l’environnement des exécuteurs.

EN: distribute `spark_jev.py` and `jev_common.py` to executors, then register the function. It is marked nondeterministic to prevent incorrect SQL optimization. Filter candidate rows before remote calls and set the Jev key in executor environments.

ES: distribuya `spark_jev.py` y `jev_common.py` a los ejecutores y registre la función. Se marca como no determinista para evitar optimizaciones SQL incorrectas. Filtre las filas candidatas antes de las llamadas remotas y defina la clave Jev en los ejecutores.

```python
spark.sparkContext.addPyFile('jev_common.py')
spark.sparkContext.addPyFile('spark_jev.py')
from spark_jev import register_jev_if
register_jev_if(spark, 'Does the review recommend the movie?', max_calls_per_worker=500)
spark.sql("SELECT review, jev_if(review).route AS route FROM reviews WHERE review IS NOT NULL")
```

## TLS / TLS / TLS

FR : si votre installation Python ne trouve pas les certificats racines, définissez `SSL_CERT_FILE` vers un bundle CA valide (par exemple `certifi.where()`). Ne désactivez pas la vérification TLS.

EN: if Python cannot find root certificates, set `SSL_CERT_FILE` to a valid CA bundle (for example `certifi.where()`). Keep TLS verification enabled.

ES: si Python no encuentra los certificados raíz, defina `SSL_CERT_FILE` con un paquete CA válido (por ejemplo `certifi.where()`). Mantenga activa la verificación TLS.

## Development / Développement / Desarrollo

`python -m unittest discover -p "test_*.py" -v`

Platform / Plateforme / Plataforma: [Apache Spark documentation](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html).

MIT license. Community project; not an official Apache Spark integration.
