CREATE TABLE `fruits` (
  `fruit_name` varchar(20) NOT NULL,
  `color` varchar(20) DEFAULT NULL,
  `price` int DEFAULT NULL,
  PRIMARY KEY (`fruit_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `fruits` VALUES ('apple','red',42);
