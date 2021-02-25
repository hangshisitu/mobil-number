/*
 Navicat Premium Data Transfer

 Source Server         : test
 Source Server Type    : SQLite
 Source Server Version : 3008008
 Source Database       : main

 Target Server Type    : SQLite
 Target Server Version : 3008008
 File Encoding         : utf-8

 Date: 05/04/2020 15:42:34 PM
*/

PRAGMA foreign_keys = false;

-- ----------------------------
--  Table structure for t_area_division
-- ----------------------------
DROP TABLE IF EXISTS "t_area_division";
CREATE TABLE t_area_division(
  id int NOT NULL PRIMARY KEY,
  name varchar(128) NOT NULL DEFAULT '',
  code int(11) NOT NULL DEFAULT '0',
  full_code int(11) NOT NULL DEFAULT '0',
  level tinyint(3) NOT NULL DEFAULT '0',
  parent_code int(11) NOT NULL DEFAULT '0',
  ctime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  utime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  );

PRAGMA foreign_keys = true;
