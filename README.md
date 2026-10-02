# ads_explorer
Personal project - focus on extract ads from many kind of marketplaces and union information about any item/vehicle/object

## Supabase SCD2 sync

The extraction notebook creates `df_anuncios`. Run the SCD2 migration in `supabase_scd2.sql` once, then configure the environment:

1. Install dependencies in the notebook environment: `pip install supabase python-dotenv`.
2. Copy `.env.example` to `.env` and set `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` from the Supabase project.
3. Keep `.env` private. The service-role key bypasses RLS; never put it in a notebook, commit, frontend, or shared logs.
4. In a notebook cell after extraction, run:

```python
from supabase_sync import sync_ads_dataframe

resultado = sync_ads_dataframe(df_anuncios)
print(resultado)
```

The sync skips extraction failures, deduplicates each batch by `anuncio_listId` (falling back to `anuncio_adId`), and compares `anuncio_price` (falling back to `preco_card`). New ads are inserted at version 1; a changed price marks the previous version non-current and inserts the next version. An unchanged price is skipped.
