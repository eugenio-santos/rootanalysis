127.0.0.1 3306 root root



CREATE TABLE gastos_train (
    GroupKey VARCHAR(255) NULL ,
    PrimaryKey VARCHAR(255) NULL ,
    RelationKey VARCHAR(255) NULL ,
    RelationType VARCHAR(255) NULL ,
    Date VARCHAR(255) NULL ,
    Value VARCHAR(255) NULL ,
    PrimaryKeyGroupAccountName VARCHAR(255) NULL ,
    RelationKeyGroupAccountName VARCHAR(255) NULL ,
    anomaly VARCHAR(255) NULL
);

CREATE TABLE gastos_trial (
    GroupKey VARCHAR(255) NULL,
    PrimaryKey VARCHAR(255) NULL,
    RelationKey VARCHAR(255) NULL,
    RelationType VARCHAR(255) NULL,
    Date INT NULL,
    Value  NULL
);

SELECT gastos_train.PrimaryKey, gastos_train.RelationKey, gastos_train.RelationType, gastos_train.Date, gastos_train.Value  FROM gastos_train WHERE gastos_train.anomaly = '1' AND gastos_train.GroupKey = 'A1';


SELECT gastos_train.PrimaryKey ,COUNT(*) FROM gastos_train GROUP BY gastos_train.PrimaryKey;


SELECT * FROM gastos_trial WHERE gastos_trial.PrimaryKey = 'A1' OR gastos_trial.PrimaryKey = 'A2';



SELECT * 
FROM (
	SELECT * 
	FROM gastos_trial 
	WHERE gastos_trial.PrimaryKey NOT IN (
        SELECT gastos_trial.RelationKey 
        FROM gastos_trial 
        WHERE gastos_trial.GroupKey = "A1" and gastos_trial.Date = 201701) 
		AND gastos_trial.GroupKey = "A1" and gastos_trial.Date = 201701) AS A
WHERE A.GroupKey = "A1" AND A.Date = 201701 AND A.relationkey != '' 
ORDER BY VALUE DESC;

SELECT gastos_trial.RelationKey FROM gastos_trial WHERE gastos_trial.GroupKey = "A1" and gastos_trial.Date = 201701;

SELECT * FROM gastos_trial WHERE gastos_trial.PrimaryKey NOT IN (SELECT gastos_trial.RelationKey FROM gastos_trial WHERE gastos_trial.GroupKey = "A1" and gastos_trial.Date = 201701) AND gastos_trial.GroupKey = "A1"  and gastos_trial.Date = 201701;


#
SELECT * 
FROM (
	SELECT * 
	FROM gastos_train 
	WHERE gastos_train.PrimaryKey NOT IN (
        SELECT gastos_train.RelationKey 
        FROM gastos_train 
        WHERE gastos_train.GroupKey = "A1" and gastos_train.Date = 201701) 
		AND gastos_train.GroupKey = "A1" and gastos_train.Date = 201701) AS A
WHERE A.GroupKey = "A1" AND A.Date = 201701 AND A.relationkey != '' 
ORDER BY VALUE DESC;