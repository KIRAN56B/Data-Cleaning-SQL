-- Data Cleaning

SELECT *
FROM layoffs;

-- 1 Remove Duplicates
-- 2 Standardize the data
-- 3 Null values or Blank values
-- 4 Remove any columns which is unnesscary 

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging	
SELECT *
FROM layoffs;


SELECT *,
ROW_NUMBER() Over(
Partition by company , location, industry , total_laid_off, percentage_laid_off, 'date' , stage , country , funds_raised_millions) as row_num
FROM layoffs_STAGING;

WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER() Over(
Partition by company , location, industry , total_laid_off, percentage_laid_off, 'date' , stage , country , funds_raised_millions) as row_num
FROM layoffs_STAGING
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company ='casper';


WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER() Over(
Partition by company , location, industry , total_laid_off, percentage_laid_off, 'date', stage , country , funds_raised_millions) as row_num
FROM layoffs_STAGING
)
Delete
FROM duplicate_cte
WHERE row_num > 1;



CREATE TABLE `layoffs_staging2` (
  `company` text ,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT	
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() Over(
Partition by company , location, industry , total_laid_off, percentage_laid_off, 'date', stage , country , funds_raised_millions) as row_num
FROM layoffs_STAGING;

Delete
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- standardizing the data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company) ;

SELECT distinct INDUSTRY
FROM layoffs_staging2
;

UPDATE layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%' ;

SELECT distinct country 
FROM layoffs_staging2
where country like 'united states%'
order by 1;

SELECT distinct country , trim(TRAILING '.' FROM COUNTRY )
FROM layoffs_staging2
where country like 'united states%'
order by 1;

UPDATE LAYOFFS_STAGING2 
SET country = TRIM(Trailing '.' FROM country)
where country LIKE 'United States%';
 
 SELECT `date` ,
 str_to_date(`date` , '%m/%d/%Y')
FROM layoffs_staging2;
 
 UPDATE layoffs_staging2
 SET  `date` =  STR_TO_DATE( `date` , '%m/%d/%Y');
 
ALTER TABLE layoffs_staging2
 MODIFY COLUMN `date` DATE;
 
 SELECT*
 FROM layoffs_staging2
 where total_laid_off is NULL
 and percentage_laid_off is null;
 
 UPDATE layoffs_staging2
 set industry = NULL
 Where industry = '';
 
 
 
 
 
  SELECT *
 FROM layoffs_staging2
 Where industry is NULL
 or industry = '';
 
 
  SELECT *
 FROM layoffs_staging2
 Where COMPANY = 'AIRBNB';
 
 
 
 SELECT*
 FROM layoffs_staging2 t1
 JOIN layoffs_staging2 t2
     ON t1.company = t2.company

	where(t1.industry is NULL OR t1.industry = '')
     AND t2.industry is NOT NULL;
 
 UPDATE layoffs_staging2  t1
 JOIN layoffs_staging2 t2
 ON t1.company = t2.company
 SET t1.industry = t2.industry 
 WHERE (t1.industry IS NULL OR t1.industry = '')
 and t2.industry is not null;
 
 
   SELECT *
 FROM layoffs_staging2;

 
  SELECT*
 FROM layoffs_staging2
 where total_laid_off is NULL
 and percentage_laid_off is null;
 
 DELETE 
 FROM layoffs_staging2
 where total_laid_off is NULL
 and percentage_laid_off is null;
 
 
SELECT*
FROM layoffs_staging2;

 alter table layoffs_staging2
 DROP COLUMN row_num;
 
 
 
 
 
 
 
 
 
 
 
 
 
 