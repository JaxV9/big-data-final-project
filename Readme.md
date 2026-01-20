## Étapes du setup

- Exécuter le docker-compose
- À partir de la racine faire "python3 ./flows/config.py"
- Générer données "python3 ./script/generate_data.py"
- Exécuter flows/ bronze, silver et gold
- Se rendre sur 'http://localhost:8000/generatedata',

- Lire le contenu de Mongo sur 'http://localhost:8000/amount_per_month'
- Pour visualiser le benchmark:
    - Exécuter à la racine avec la commande 'streamlit run ./streamlit/app.py' dans streamlit/app.py
    - Se rendre sur l'url indiquée dans les logs




## Benchmark
![alt text](image-1.png)