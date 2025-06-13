import yaml
import zipfile
import pandas as pd
# from pathlib import Path
from typing import Dict, List, Any, Optional
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
# from prefect.futures import PrefectFuture
import tempfile
import os
from dataclasses import dataclass

# Configuration pour la concurrence
TASK_RUNNER = ConcurrentTaskRunner(max_workers=10)

@dataclass
class DataArtifact:
    """Représente un artefact de données qui transite entre les tasks"""
    file_path: str
    dataset_name: str
    metadata: Dict[str, Any]
    sheet_name: Optional[str] = None
    page_info: Optional[Dict] = None

@task(name="extract_pages")
def extract_dataset_pages(extractor, dataset_config: Dict[str, Any]) -> List[DataArtifact]:
    """Extrait toutes les pages d'un dataset et yield des artefacts"""
    dataset_name = dataset_config["name"]
    print(f"🔄 Extraction par pages de {dataset_name}...")
    
    artifacts = []
    page_num = 0
    
    # Utilise le générateur de l'extractor pour récupérer chaque page
    for page_result in extractor.execute():
        page_num += 1
        artifact = DataArtifact(
            file_path=page_result,
            dataset_name=dataset_name,
            metadata=dataset_config,
            page_info={"page": page_num, "total_pages": "unknown"}
        )
        artifacts.append(artifact)
        print(f"   📄 Page {page_num} extraite")
    
    print(f"✅ {dataset_name}: {len(artifacts)} pages extraites")
    return artifacts

@task(name="unzip_artifacts")
def unzip_artifact(artifact: DataArtifact) -> List[DataArtifact]:
    """Dézippe un artefact si nécessaire et yield les fichiers extraits"""
    if not artifact.file_path.lower().endswith('.zip'):
        return [artifact]
    
    print(f"📦 Dézippage: {artifact.dataset_name} - page {artifact.page_info.get('page', 'N/A')}")
    
    extract_dir = tempfile.mkdtemp()
    extracted_artifacts = []
    
    with zipfile.ZipFile(artifact.file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    
    # Crée un artefact pour chaque fichier extrait
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            if file.lower().endswith(('.csv', '.xlsx', '.json')):
                file_path = os.path.join(root, file)
                new_artifact = DataArtifact(
                    file_path=file_path,
                    dataset_name=artifact.dataset_name,
                    metadata=artifact.metadata,
                    page_info=artifact.page_info
                )
                extracted_artifacts.append(new_artifact)
    
    print(f"   ✅ {len(extracted_artifacts)} fichiers extraits du zip")
    return extracted_artifacts

@task(name="get_excel_sheets")
def extract_excel_sheets(artifact: DataArtifact) -> List[DataArtifact]:
    """Extrait les feuilles d'un fichier Excel et yield un artefact par feuille"""
    if not artifact.file_path.lower().endswith('.xlsx'):
        return [artifact]
    
    print(f"📊 Extraction feuilles Excel: {artifact.dataset_name}")
    
    excel_file = pd.ExcelFile(artifact.file_path)
    sheet_artifacts = []
    
    for sheet_name in excel_file.sheet_names:
        print(f"   📋 Feuille: {sheet_name}")
        
        # Crée un artefact pour chaque feuille
        sheet_artifact = DataArtifact(
            file_path=artifact.file_path,
            dataset_name=artifact.dataset_name,
            metadata=artifact.metadata,
            sheet_name=sheet_name,
            page_info=artifact.page_info
        )
        sheet_artifacts.append(sheet_artifact)
    
    return sheet_artifacts

@task(name="trim_data")
def trim_artifact_data(artifact: DataArtifact) -> DataArtifact:
    """Applique le trimming sur un artefact et yield l'artefact nettoyé"""
    print(f"✂️  Trimming: {artifact.dataset_name} - {artifact.sheet_name or 'fichier principal'}")
    
    # Détermine la config de trimming
    trim_config = None
    if artifact.file_path.lower().endswith('.xlsx') and artifact.sheet_name:
        trim_config = artifact.metadata.get('excel_trim', {}).get(artifact.sheet_name)
    elif artifact.file_path.lower().endswith('.csv'):
        trim_config = artifact.metadata.get('csv_trim')
    
    if not trim_config:
        return artifact  # Pas de trimming nécessaire
    
    # Lit le fichier selon son type
    if artifact.file_path.lower().endswith('.xlsx'):
        df = pd.read_excel(artifact.file_path, sheet_name=artifact.sheet_name)
    elif artifact.file_path.lower().endswith('.csv'):
        df = pd.read_csv(artifact.file_path)
    else:
        return artifact  # Pas de trimming pour JSON
    
    # Applique le trimming
    if 'skip_rows' in trim_config:
        df = df.iloc[trim_config['skip_rows']:]
    if 'skip_footer' in trim_config:
        df = df.iloc[:-trim_config['skip_footer']]
    
    # Sauvegarde le fichier nettoyé
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()
    
    # Retourne un nouvel artefact avec le fichier nettoyé
    cleaned_artifact = DataArtifact(
        file_path=temp_file.name,
        dataset_name=artifact.dataset_name,
        metadata=artifact.metadata,
        sheet_name=artifact.sheet_name,
        page_info=artifact.page_info
    )
    
    print(f"   ✅ Trimming appliqué, {len(df)} lignes conservées")
    return cleaned_artifact

@task(name="load_artifact")
def load_single_artifact(loader, artifact: DataArtifact) -> Dict[str, Any]:
    """Charge un artefact unique en base et yield le résultat"""
    # Construit le nom de table
    table_name = artifact.dataset_name
    if artifact.sheet_name:
        table_name += f"_{artifact.sheet_name}"
    if artifact.page_info and artifact.page_info.get('page', 1) > 1:
        table_name += f"_page{artifact.page_info['page']}"
    
    print(f"💾 Chargement: {table_name}")
    
    try:
        # Utilise le générateur du loader
        for result in loader.execute(artifact.file_path):
            pass  # Le loader fait son travail
        
        result = {
            "table_name": table_name,
            "dataset": artifact.dataset_name,
            "sheet": artifact.sheet_name,
            "page": artifact.page_info.get('page') if artifact.page_info else None,
            "status": "success",
            "file_path": artifact.file_path
        }
        
        print(f"✅ {table_name} chargé avec succès")
        return result
        
    except Exception as e:
        result = {
            "table_name": table_name,
            "dataset": artifact.dataset_name,
            "sheet": artifact.sheet_name,
            "page": artifact.page_info.get('page') if artifact.page_info else None,
            "status": "error",
            "error": str(e),
            "file_path": artifact.file_path
        }
        
        print(f"❌ Erreur chargement {table_name}: {str(e)}")
        return result

@flow(name="Dataset Processing Pipeline", task_runner=TASK_RUNNER)
def process_single_dataset_flow(
    dataset_config: Dict[str, Any],
    extractor_class,
    loader_class
) -> List[Dict[str, Any]]:
    """
    Flow pour traiter un dataset complet avec pipeline concurrent
    Chaque étape peut traiter plusieurs artefacts en parallèle
    """
    dataset_name = dataset_config["name"]
    print(f"🚀 Démarrage pipeline pour: {dataset_name}")
    
    # Instancie l'extractor et le loader
    extractor = extractor_class(dataset_config)
    loader = loader_class(dataset_config)
    
    # 1. EXTRACTION - Récupère toutes les pages
    extract_future = extract_dataset_pages.submit(extractor, dataset_config)
    page_artifacts = extract_future.result()
    
    # 2. UNZIP - Traite chaque page en parallèle
    unzip_futures = []
    for artifact in page_artifacts:
        future = unzip_artifact.submit(artifact)
        unzip_futures.append(future)
    
    # Collecte tous les fichiers dézippés
    all_unzipped = []
    for future in unzip_futures:
        all_unzipped.extend(future.result())
    
    # 3. EXCEL SHEETS - Extrait les feuilles en parallèle
    sheet_futures = []
    for artifact in all_unzipped:
        future = extract_excel_sheets.submit(artifact)
        sheet_futures.append(future)
    
    # Collecte toutes les feuilles
    all_sheets = []
    for future in sheet_futures:
        all_sheets.extend(future.result())
    
    # 4. TRIMMING - Applique le nettoyage en parallèle
    trim_futures = []
    for artifact in all_sheets:
        future = trim_artifact_data.submit(artifact)
        trim_futures.append(future)
    
    # Collecte tous les artefacts nettoyés
    cleaned_artifacts = []
    for future in trim_futures:
        cleaned_artifacts.append(future.result())
    
    # 5. LOADING - Charge tout en parallèle
    load_futures = []
    for artifact in cleaned_artifacts:
        future = load_single_artifact.submit(loader, artifact)
        load_futures.append(future)
    
    # Collecte tous les résultats de chargement
    load_results = []
    for future in load_futures:
        load_results.append(future.result())
    
    print(f"✅ Pipeline {dataset_name} terminé: {len(load_results)} artefacts traités")
    return load_results

@flow(name="Master ETL Pipeline", task_runner=TASK_RUNNER)
def master_etl_pipeline(
    config_path: str,
    extractor_class,
    loader_class
) -> Dict[str, Any]:
    """
    Flow maître qui orchestre tous les datasets en parallèle
    """
    print("🚀 Démarrage du Master ETL Pipeline")
    
    # Charge la configuration
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    datasets = config.get("datasets", [])
    print(f"📊 {len(datasets)} datasets à traiter")
    
    # Lance un sous-flow pour chaque dataset
    dataset_futures = []
    for dataset_config in datasets:
        future = process_single_dataset_flow.submit(
            dataset_config,
            extractor_class,
            loader_class
        )
        dataset_futures.append((dataset_config["name"], future))
    
    # Collecte tous les résultats
    all_results = {}
    for dataset_name, future in dataset_futures:
        try:
            results = future.result()
            all_results[dataset_name] = results
        except Exception as e:
            print(f"❌ Erreur pour dataset {dataset_name}: {str(e)}")
            all_results[dataset_name] = [{"status": "error", "error": str(e)}]
    
    # Calcule les statistiques globales
    total_artifacts = 0
    successful_artifacts = 0
    failed_artifacts = 0
    
    for dataset_name, results in all_results.items():
        for result in results:
            total_artifacts += 1
            if result["status"] == "success":
                successful_artifacts += 1
            else:
                failed_artifacts += 1
    
    summary = {
        "total_datasets": len(datasets),
        "total_artifacts": total_artifacts,
        "successful_artifacts": successful_artifacts,
        "failed_artifacts": failed_artifacts,
        "success_rate": f"{(successful_artifacts/total_artifacts*100):.1f}%" if total_artifacts > 0 else "0%",
        "details": all_results
    }
    
    print(f"""
🎯 Master Pipeline terminé !
   📊 Datasets: {len(datasets)}
   📄 Artefacts totaux: {total_artifacts}
   ✅ Succès: {successful_artifacts}
   ❌ Échecs: {failed_artifacts}
   📈 Taux de réussite: {summary['success_rate']}
    """)
    
    return summary

# Exemple d'utilisation
if __name__ == "__main__":
    # Importe tes classes
    # from your_module import YourExtractorClass, YourLoaderClass
    
    # Lance le pipeline maître
    # result = master_etl_pipeline(
    #     config_path="datasets_config.yaml",
    #     extractor_class=YourExtractorClass,
    #     loader_class=YourLoaderClass
    # )
    
    print("""
🚀 Pipeline ETL Concurrent prêt !

Architecture:
- Master Flow: orchestre tous les datasets
- Dataset Flow: traite un dataset avec pipeline concurrent  
- Tasks: extract_pages → unzip → get_sheets → trim → load
- Chaque étape traite ses artefacts en parallèle
- Concurrence maximale à tous les niveaux !
    """)