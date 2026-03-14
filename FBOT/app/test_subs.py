import datetime
from datetime import timedelta, timezone

TZ_KZ = timezone(timedelta(hours=5))

def is_subscribed_mock(expires_at):
    if not expires_at:
        return False
    now = datetime.datetime.now(TZ_KZ)
    if expires_at.tzinfo is None:
        return expires_at > now.replace(tzinfo=None)
    return expires_at > now.astimezone(expires_at.tzinfo)

def is_subscribed_fixed(expires_at):
    if not expires_at:
        return False
    now = datetime.datetime.now(TZ_KZ)
    if expires_at.tzinfo is None:
        return expires_at > now.replace(tzinfo=None)
    return expires_at > now.astimezone(expires_at.tzinfo)

# Тест 5: Проверка исправленной логики (UTC vs KZ)
now_kz = datetime.datetime.now(TZ_KZ)
ext_utc = datetime.datetime.now(timezone.utc) + timedelta(hours=1)
print(f"Test 5 (Fixed comparison): expired={not is_subscribed_fixed(ext_utc)}")
print(f"  now (KZ): {now_kz}")
print(f"  expires_at (UTC): {ext_utc}")
print(f"  Result: {'Active' if is_subscribed_fixed(ext_utc) else 'Expired'}")
