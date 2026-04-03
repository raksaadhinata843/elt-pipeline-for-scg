.output DATA_DOCS.md

ATTACH 'md:my_db';

-- Header Utama
.print '# Data Documentation (Medallion - Gold Layer)'
.print 'Last Updated: '
SELECT now();

-- Tabel Schema
.print ''
.print '### Tabel Gabungan (Cement, Coal, Oil)'
.mode markdown
DESCRIBE my_db.gold_unified;

-- Print Statistik
.print ''
.print '### Statistik Data'
.mode markdown
SUMMARIZE my_db.gold_unified;

.output
