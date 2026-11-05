# Пример decoy-сайта

Статический сайт для `[web.vhosts.decoy]` в режиме `static_directory`. Нужен, чтобы запрос без валидной capability (активное пробирование, ручная проверка домена) видел обычный сайт, а не служебную заглушку.

```toml
[[web.vhosts]]
host = "proxy.example.com"
public_addr = "203.0.113.10:443"

[web.vhosts.decoy]
mode = "static_directory"
directory = "/opt/telemt-web/decoy"
index = "index.html"
```

Каталог `contrib/decoy-site` копируется в `directory` целиком: `index.html`, `assets/style.css`, `assets/logo.svg`. Тексты и контакты — плейсхолдеры, замените на свои.

Ограничения: `max_static_files` (4096), `max_static_file_bytes` (8 МиБ), `max_static_bytes` (64 МиБ). Каталог читается при загрузке конфига, поэтому правки файлов требуют перезагрузки конфига или перезапуска процесса.
