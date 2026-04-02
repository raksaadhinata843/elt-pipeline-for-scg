-- Pindah ke database MotherDuck
ATTACH 'md:my_db';

-- Generate tabel
PRINT '### Tabel Gabungan (Cement, Coal, Oil)';
DESCRIBE my_db.gold_unified;

-- Generate statistik data 
PRINT '### Statistik Data';
SUMMARIZE my_db.gold_unified;

