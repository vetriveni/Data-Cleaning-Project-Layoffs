select *,
row_number() over(
partition by company,location,industry,
total_laid_off,percentage_laid_off,`date`,stage,country
,funds_raised_millions)as row_num
from layoffs_staging;
with duplicate_cte as
(
select *,
row_number() over(
partition by company,location,industry,
total_laid_off,percentage_laid_off,`date`,stage,country
,funds_raised_millions)as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num>1;
drop table layoffs_staging2
insert into layoffs_staging2
select *,
row_number() over(
partition by company,location,industry,
total_laid_off,percentage_laid_off,`date`,stage,country
,funds_raised_millions)as row_num
from layoffs_staging
select * from layoffs_staging2
where row_num>1;
select distinct(company)
from layoffs_staging2;

select distinct(trim(company))
from layoffs_staging2;
select company,trim(company)
from layoffs_staging2;
select distinct industry
from layoffs_staging2
order by 1;
select * from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry='crypto'
where industry like 'crypto%';

select * from layoffs_staging2 where industry='crypto_currency'



update layoffs_staging2
set company=trim(company);
select distinct(location)
from layoffs_staging2
order by 1;
select distinct country
from layoffs_staging2
order by 1;
select *
from layoffs_staging2
where country like 'united states%';

select distinct country,trim(trailing '.' from country)
from layoffs_staging2
order by 1;
update layoffs_staging2
set country=trim(trailing '.' from country)
where country like 'united states%';
select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;
update layoffs_staging2
set `date`=str_to_date(`date`,'%m/%d/%Y');

select `date` from layoffs_staging2;
alter table layoffs_staging2
modify column `date` date;

select *
 from layoffs_staging2 
 where total_laid_off is null
 and percentage_laid_off is null;

select *
from layoffs_staging2 
where industry is null
or industry='';

select * from layoffs_staging2
where company='Airbnb';
select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
where(t1.industry is null or t1.industry='')
and t2.industry is not null;
update layoffs_staging2
set industry=null
where industry='';

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
set t1.industry=t2.industry
where(t1.industry is null )
and t2.industry is not null;


select * from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;


delete from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2

















