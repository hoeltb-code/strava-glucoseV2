-- À exécuter une seule fois sur la base de production lors du lancement
-- de l'offre premier plan. Les achats de packs effectués ultérieurement
-- continueront naturellement à créditer les comptes concernés.
UPDATE plan_credit_wallets
SET credits = 0,
    updated_at = CURRENT_TIMESTAMP;
