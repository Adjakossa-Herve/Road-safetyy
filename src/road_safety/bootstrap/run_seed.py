"""Script utilitaire pour lancer le seed manuellement."""

from road_safety.bootstrap.data_seed import ensure_accidents_loaded

print("Démarrage du seed...")
ensure_accidents_loaded()
print("Seed terminé.")
