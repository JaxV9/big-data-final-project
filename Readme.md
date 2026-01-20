## Étapes du setup

- Exécuter le docker-compose
- Exécuter flows/config.py
- Générer données avec script/generate_data
- Exécuter flows/ bronze, silver et gold
- Se rendre sur 'http://localhost:8000/generatedata' pour insérer des données de MinIO vers Mongo
- Lire le contenu de Mongo sur 'http://localhost:8000/amount_per_month'
- Pour visualiser le benchmark:
    - Exécuter avec la commande 'streamlit run app.py' dans streamlit/app.py
    - Se rendre sur l'url indiquée dans les logs





![alt text](image-1.png)