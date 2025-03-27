import os
import traceback
import boto3
import pandas as pd

from botocore.exceptions import NoCredentialsError
from io import BytesIO


def get_data_file_path(file_name): 
    # Définir le répertoire data local pour ce notebook
    notebook_dir = os.path.abspath('')
    print(f"🧾 notebook_dir: {notebook_dir}")
    data_folder = os.path.join(os.path.dirname(notebook_dir), 'data/imports/')
    print(f"🧾 file_folder: {data_folder}")
    file_path = os.path.join(data_folder, file_name)
    print(f"🧾 file_path: {file_path}")
    # Vérifier si le fichier existe
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Fichier introuvable : {file_path}")
    print(f"✔️ Fichier trouvé.")
    print(f"✔️ Emplacement: {file_path}")
    return file_path

def list_s3_objects(bucket_name):
    """
    List the objects in a given S3 bucket and print their details (name, last modified, size).
    
    :param bucket_name: The name of the S3 bucket.
    """
    # Initialize the S3 client
    s3_client = boto3.client('s3')

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in response:
            print(f"Les fichiers présents dans le bucket '{bucket_name}' sont :")
            for obj in response['Contents']:
                print(f"Nom de l'objet: {obj['Key']}, Dernière modification: {obj['LastModified']}, Taille: {obj['Size']} octets")
        else:
            print(f"Le bucket '{bucket_name}' est vide.")
    
    except NoCredentialsError:
        print("❌ Les informations d'identification sont manquantes ou incorrectes.")
    
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des fichiers : {e}")

def read_excel_from_s3(s3_client, bucket_name, file_path):
    """
    Récupère un fichier Excel depuis un bucket S3 et le charge sous forme de flux
    
    :param s3_client: Client S3 configuré.
    :param bucket_name: Nom du bucket S3.
    :param file_path: Clé (chemin) du fichier dans le bucket S3.
    :return: Un objet fichier sous forme de flux.
    """
    
    try:
        # Retrieve the file from S3 as a binary stream
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        file_content = response['Body'].read()
        
        # Use BytesIO to create a file-like object from the binary data
        file_stream = BytesIO(file_content)
        
        return file_stream

    except NoCredentialsError:
        print("❌ Les informations d'identification sont manquantes ou incorrectes.")
    except Exception as e:
        print(f"❌ Erreur lors de la récupération du fichier : {e}")
        return None