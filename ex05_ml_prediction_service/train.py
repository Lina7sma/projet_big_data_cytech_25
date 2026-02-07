import os
import pandas as pd
import joblib
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error
import numpy as np

def load_data():
    """
    Charge les données depuis la base de données PostgreSQL (Couche Gold).

    Returns
    -------
    pd.DataFrame
        Le dataset contenant les trajets de taxi pour l'entraînement.
    """

    IS_DOCKER = os.path.exists('/.dockerenv')
    DB_HOST = "postgres" if IS_DOCKER else "localhost"

    # Utilisation de la variable DB_HOST dans l'URL
    engine = create_engine(f"postgresql://postgres:postgres@{DB_HOST}:5432/postgres")

    # On prend les colonnes clés pour la prédiction
    query = """
            SELECT trip_distance, pickup_location_id, dropoff_location_id, EXTRACT(HOUR FROM pickup_datetime) as pickup_hour,
                   EXTRACT(DOW FROM pickup_datetime) as day_of_week, total_amount
            FROM fact_trips
            WHERE total_amount > 0 AND total_amount < 500
            LIMIT 1000000 \
            """
    return pd.read_sql(query, engine)

def train_and_evaluate(df):
    """
    Entraîne un modèle Random Forest et calcule sa performance.

    Parameters
    ----------
    df : pd.DataFrame
        Le dataframe contenant les features et la cible (total_amount).

    Returns
    -------
    model : RandomForestRegressor
        Le modèle entraîné.
    rmse : float
        L'erreur moyenne du modèle (doit être < 10$).
    """
    # Préparation des données
    X = df[['trip_distance', 'pickup_location_id', 'dropoff_location_id', 'pickup_hour', 'day_of_week']]
    y = df['total_amount']

    # Découpage 80% entraînement / 20% test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Création et entraînement du modèle (100 arbres pour plus de précision)
    print(">>> Entraînement du modèle en cours... (Cela peut prendre quelques minutes)")
    model = RandomForestRegressor(n_estimators=100, max_depth=15, random_state=42)
    model.fit(X_train, y_train)

    # Calcul de l'erreur
    predictions = model.predict(X_test)
    rmse = root_mean_squared_error(y_test, predictions)

    return model, rmse

def run_unit_tests(model):
    """
    Exécute des tests unitaires pour valider la cohérence des prédictions.

    Parameters
    ----------
    model : RandomForestRegressor
        Le modèle à tester.
    """
    print(">>> Lancement des tests unitaires sur le modèle...")

    # Test 1: Une distance courte doit avoir un prix raisonnable
    test_data = pd.DataFrame([[1.0, 132, 132, 14, 5]],
                             columns=['trip_distance', 'pickup_location_id', 'dropoff_location_id', 'pickup_hour', 'day_of_week'])

    prediction = model.predict(test_data)[0]
    assert prediction > 0, "Test échoué : prix négatif !"
    assert prediction < 100, f"Test échoué : prix trop élevé pour 1 mile ({prediction}$)"

    print("✅ Tous les tests unitaires sont passés avec succès !")

if __name__ == "__main__":
    # 1. Chargement
    print("🚀 Démarrage du Pipeline de Machine Learning")
    data = load_data()

    # 2. Entraînement
    model, rmse_score = train_and_evaluate(data)
    print(f"📊 Performance du modèle (RMSE) : {rmse_score:.2f}$")
    print(f"💡 Objectif prof (< 10$) : {'REUSSI ✅' if rmse_score < 10 else 'A REVOIR ❌'}")

    # 3. Tests unitaires (Exigence Prof)
    run_unit_tests(model)
    model.rmse_score = round(rmse_score, 2)
    # 4. Sauvegarde (Pour le Dashboard)
    joblib.dump(model, "taxi_model.joblib")
    print(f"💾 Modèle sauvegardé sous 'taxi_model.joblib' avec la valeur rmse {model.rmse_score}")