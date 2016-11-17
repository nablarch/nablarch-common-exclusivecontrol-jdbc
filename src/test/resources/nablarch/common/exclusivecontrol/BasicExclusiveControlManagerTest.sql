-- 
-- 条件指定によるユーザ検索
--
UPDATE_USER_NAME = 
UPDATE
  USER_MST
SET
  NAME = :name
WHERE
  USER_ID = :user_id
  AND PK2 = :pk2
  AND PK3 = :pk3
