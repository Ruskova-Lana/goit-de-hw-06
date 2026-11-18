"""Завантаження Kafka-конектора для Spark у папку jars/ (для запуску без інтернету).

Запускати там, де є інтернет (наприклад, у Windows):
    python download_jars.py                      # версія = встановлений pyspark
    python download_jars.py --spark-version 4.3.0

Скрипт читає POM-файли конектора з Maven Central і завантажує його залежності.
Бібліотеки, які вже входять у PySpark (pyspark/jars), пропускаються.
Після цього spark_alerts.py автоматично використовує jars/ замість завантаження.
"""

import argparse
import os
import re
import urllib.request
import xml.etree.ElementTree as ET

MAVEN = "https://repo1.maven.org/maven2"
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
JARS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars")

_pom_cache: dict[tuple[str, str, str], ET.Element] = {}


def artifact_url(group: str, artifact: str, version: str, ext: str) -> str:
    return f"{MAVEN}/{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.{ext}"


def fetch_pom(group: str, artifact: str, version: str) -> ET.Element:
    key = (group, artifact, version)
    if key not in _pom_cache:
        with urllib.request.urlopen(artifact_url(group, artifact, version, "pom"), timeout=60) as resp:
            _pom_cache[key] = ET.fromstring(resp.read())
    return _pom_cache[key]


def text(node, path: str) -> str | None:
    found = node.find(path, NS)
    return found.text.strip() if found is not None and found.text else None


def pom_context(pom: ET.Element) -> tuple[dict, dict]:
    """Properties та dependencyManagement з урахуванням батьківських POM."""
    props, managed = {}, {}
    parent = pom.find("m:parent", NS)
    if parent is not None:
        props, managed = pom_context(
            fetch_pom(text(parent, "m:groupId"), text(parent, "m:artifactId"), text(parent, "m:version"))
        )
    version = text(pom, "m:version") or text(pom, "m:parent/m:version")
    props.update({"project.version": version, "version": version})
    for prop in pom.findall("m:properties/*", NS):
        props[prop.tag.split("}")[1]] = (prop.text or "").strip()
    for dep in pom.findall("m:dependencyManagement/m:dependencies/m:dependency", NS):
        managed[(text(dep, "m:groupId"), text(dep, "m:artifactId"))] = text(dep, "m:version")
    return props, managed


def resolve(value: str | None, props: dict) -> str | None:
    for _ in range(10):
        if value is None or "${" not in value:
            return value
        value = re.sub(r"\$\{([^}]+)\}", lambda m: props.get(m.group(1), m.group(0)), value)
    return value


def bundled_artifacts() -> set[str]:
    try:
        import pyspark
    except ImportError:
        return set()
    jars = os.listdir(os.path.join(os.path.dirname(pyspark.__file__), "jars"))
    return {re.sub(r"-\d.*\.jar$", "", name) for name in jars}


def collect(group, artifact, version, bundled, result, scala):
    if artifact in bundled or (group, artifact) in result:
        return
    result[(group, artifact)] = version
    pom = fetch_pom(group, artifact, version)
    props, managed = pom_context(pom)
    props.setdefault("scala.binary.version", scala)
    for dep in pom.findall("m:dependencies/m:dependency", NS):
        scope = text(dep, "m:scope") or "compile"
        if scope not in ("compile", "runtime") or text(dep, "m:optional") == "true":
            continue
        dep_group = resolve(text(dep, "m:groupId"), props)
        dep_artifact = resolve(text(dep, "m:artifactId"), props)
        dep_version = resolve(text(dep, "m:version") or managed.get((dep_group, dep_artifact)), props)
        if dep_version and "${" not in dep_version:
            collect(dep_group, dep_artifact, dep_version, bundled, result, scala)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--spark-version", help="версія Spark (за замовчуванням — встановлений pyspark)")
    args = parser.parse_args()

    version = args.spark_version
    if not version:
        import pyspark

        version = pyspark.__version__
    scala = "2.13" if int(version.split(".")[0]) >= 4 else "2.12"

    result: dict[tuple[str, str], str] = {}
    collect("org.apache.spark", f"spark-sql-kafka-0-10_{scala}", version, bundled_artifacts(), result, scala)

    os.makedirs(JARS_DIR, exist_ok=True)
    for old in os.listdir(JARS_DIR):
        if old.endswith(".jar"):
            os.remove(os.path.join(JARS_DIR, old))

    for (group, artifact), dep_version in sorted(result.items()):
        target = os.path.join(JARS_DIR, f"{artifact}-{dep_version}.jar")
        print(f"Downloading {group}:{artifact}:{dep_version}")
        urllib.request.urlretrieve(artifact_url(group, artifact, dep_version, "jar"), target)

    print(f"\nDone: {len(result)} jar(s) in {JARS_DIR}")


if __name__ == "__main__":
    main()
