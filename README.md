# oil-job-bot

Azərbaycandakı neft-qaz vakansiyalarını və təcrübə proqramlarını LinkedIn-dən hər gün çəkib Telegram-a göndərən bot.

- `main.py` - bütün axın (Apify -> LLM təsnifatı -> Telegram -> saxlama)
- `config.yaml` - açar sözlər, şirkət siyahısı, ayarlar (burada dəyişirsən)
- `.github/workflows/daily.yml` - hər gün 08:00 (Bakı) avtomatik işə salır
- `data/jobs.json` - elanların yaddaşı (bot özü yeniləyir, 30 gündən köhnələri silir)

GitHub Secrets-də olmalıdır: `APIFY_TOKEN`, `OPENROUTER_API_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.
