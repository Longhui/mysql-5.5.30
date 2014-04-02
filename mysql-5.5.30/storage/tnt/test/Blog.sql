## ��������ӵ�����������䳤���Ժʹ����
CREATE TABLE Blog (
  ID BIGINT NOT NULL,
  UserID BIGINT NOT NULL,
  Title VARCHAR(255),
  Content MEDIUMTEXT,
  PublishTime BIGINT,
  AccessCount INT DEFAULT 0,
  PRIMARY KEY(ID),
  KEY IDX_BLOG_UID_PT(UserID, PublishTime),
  KEY IDX_BLOG_AC(UserID, AccessCount),
  KEY IDX_BLOG_PT(PublishTime)) ENGINE = NTSE;

## �����¼  
INSERT INTO Blog VALUES(1, 1, 'aaa', repeat('a', 10000), 1226457503000, 5);
INSERT INTO Blog VALUES(2, 1, 'bbb', repeat('b', 20000), 1226457603000, 3);
INSERT INTO Blog VALUES(3, 1, 'ccc', repeat('c', 10000), 1226457703000, 7);
INSERT INTO Blog VALUES(4, 2, 'ddd', repeat('d', 30000), 1226457103000, 3);
INSERT INTO Blog VALUES(5, 2, 'eee', repeat('e', 10000), 1226457303000, 2);
INSERT INTO Blog VALUES(6, 2, 'fff', repeat('f', 50000), 1226457503000, 5);

## ��ɨ��
EXPLAIN SELECT ID, UserID, Title, left(Content, 10), PublishTime, AccessCount FROM Blog;
SELECT ID, UserID, Title, left(Content, 10), PublishTime, AccessCount FROM Blog;

## Filesort���»���RID
SELECT ID, UserID, Title FROM Blog ORDER BY AccessCount DESC;
SELECT UserID FROM Blog ORDER BY AccessCount DESC LIMIT 3;

## ����ɨ�裬ָ������Ψһ������
EXPLAIN SELECT ID, UserID, Title, left(Content, 10), PublishTime, AccessCount FROM Blog WHERE ID = 3;
SELECT ID, UserID, Title, left(Content, 10), PublishTime, AccessCount FROM Blog WHERE ID = 3;

## ����ɨ�裬��ֵ��Ψһ����
EXPLAIN SELECT ID, UserID, Title FROM Blog WHERE UserID = 2 ORDER BY PublishTime DESC LIMIT 2 OFFSET 1;
SELECT ID, UserID, Title FROM Blog WHERE UserID = 2 ORDER BY PublishTime DESC LIMIT 2 OFFSET 1;

## ����ɨ�裬��Χ����
EXPLAIN SELECT ID, UserID, Title, PublishTime FROM Blog FORCE INDEX(IDX_BLOG_PT) WHERE PublishTime >= 1226457503000 AND PublishTime <= 1226457703000;
SELECT ID, UserID, Title, PublishTime FROM Blog FORCE INDEX(IDX_BLOG_PT) WHERE PublishTime >= 1226457503000  AND PublishTime <= 1226457703000;

## ��ɨ�貢�Ҹ��¼�¼�������漰��������
SELECT ID, UserID, Title, AccessCount FROM Blog;
UPDATE Blog SET AccessCount = AccessCount + 1;
SELECT ID, UserID, Title, AccessCount FROM Blog;
SELECT ID, UserID, Title, AccessCount FROM Blog FORCE INDEX(IDX_BLOG_AC) WHERE UserID = 1 AND AccessCount = 8;

## ����ɨ�貢�Ҹ��¼�¼�����´�����������������ɨ����������
SELECT ID, UserID, Title, AccessCount FROM Blog;
### ����ID = 3������¼
UPDATE Blog SET Title = 'cccc', Content = repeat('c', 10001), PublishTime = PublishTime + 1 WHERE UserID = 1 AND AccessCount > 7;
SELECT ID, UserID, Title, AccessCount FROM Blog;
SELECT ID, UserID, Title, length(Content) FROM Blog WHERE UserID = 1 AND AccessCount > 7;

## ����ɨ�貢�Ҹ���ɨ���������Halloween���⣩
SELECT ID, UserID, Title, AccessCount FROM Blog;
UPDATE Blog SET AccessCount = AccessCount + 1 WHERE UserID = 1 AND AccessCount > 7;
SELECT ID, UserID, Title, AccessCount FROM Blog;
SELECT ID, UserID, Title, AccessCount FROM Blog WHERE UserID = 1 AND AccessCount > 7;

## ��ɨ�貢��ɾ����¼
SELECT ID, UserID, Title, AccessCount FROM Blog;
DELETE FROM Blog WHERE Title = 'bbb';
SELECT ID, UserID, Title, AccessCount FROM Blog;

## ����ɨ�貢��ɾ����¼��ָ������Ψһ������
SELECT ID, UserID, Title, AccessCount FROM Blog;
DELETE FROM Blog WHERE ID = 3;
SELECT ID, UserID, Title, AccessCount FROM Blog;

## ����ɨ�貢��ɾ����¼��ָ����Χ����
SELECT ID, UserID, Title, AccessCount FROM Blog;
DELETE FROM Blog WHERE UserID = 2 AND AccessCount >= 3 AND AccessCount <= 5;
SELECT ID, UserID, Title, AccessCount FROM Blog;

## REPLACE
SELECT ID, UserID, Title, AccessCount FROM Blog;
REPLACE INTO Blog VALUES(1, 1, 'aaaa', repeat('a', 10000), unix_timestamp(now()) * 1000, 5);
SELECT ID, UserID, Title, AccessCount FROM Blog;
INSERT INTO Blog VALUES(1, 1, 'aaaa', repeat('a', 10000), unix_timestamp(now()) * 1000, 5) ON DUPLICATE KEY UPDATE Title = 'aaaaa';
SELECT ID, UserID, Title, AccessCount FROM Blog;

## ɾ����
DROP TABLE Blog;





