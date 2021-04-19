--DROP TYPE ARRAY
DROP TYPE freq_array@

--CREATE TYPE ARRAY
CREATE TYPE freq_array AS INTEGER ARRAY[]@

--CREATE STORED PROCEDURE
CREATE OR REPLACE PROCEDURE gen_salary_cumulativehistogram(IN start DOUBLE, IN end DOUBLE, IN number INTEGER)

LANGUAGE SQL

BEGIN 
  
    DECLARE SQLSTATE CHAR(5) DEFAULT '00000';
    DECLARE salary DOUBLE DEFAULT 0;
    DECLARE interval DOUBLE DEFAULT 0;
    DECLARE bin_index INTEGER DEFAULT 1;

    DECLARE frequency freq_array;
    DECLARE frequency_index INTEGER DEFAULT 0; 
    DECLARE frequency_cum INTEGER DEFAULT 0;

    DECLARE cursor CURSOR FOR select SALARY from employee;
    DECLARE GLOBAL TEMPORARY TABLE SESSION.freq_stat(
      binnum INTEGER,
      cumulativefrequency INTEGER,
      binstart DECIMAL(10, 2),
      binend DECIMAL(10, 2)
    ) WITH REPLACE ON COMMIT PRESERVE ROWS;

    SET interval = (end - start)/number;

    WHILE bin_index <= number DO
      SET frequency[bin_index] = 0;
      SET bin_index = bin_index + 1;
    END WHILE;

    OPEN cursor;
      FETCH FROM cursor INTO salary;
      WHILE SQLSTATE = '00000' DO
        IF salary >= start and salary < end THEN
          SET frequency_index = ((salary - start)/interval) + 1;
          SET frequency[frequency_index] = frequency[frequency_index] + 1;
        END IF;
        FETCH FROM cursor INTO salary;
      END WHILE;
    CLOSE cursor;

    SET bin_index = 1;
    WHILE bin_index <= number DO
      SET frequency_cum = frequency_cum + frequency[bin_index];
      INSERT INTO SESSION.freq_stat VALUES (bin_index, frequency_cum, start, start + interval);
      SET start = start + interval;
      SET bin_index = bin_index + 1;
    END WHILE;

END @

CALL gen_salary_cumulativehistogram(30000, 170000, 7) @

select * from SESSION.freq_stat@