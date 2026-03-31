// MongoDB queries for tiktok_reviews collection
// File: airflow/mongo_queries.js

// Top 5 frequently occurring comments
 db.tiktok_reviews.aggregate([
     { $match: { content: { $exists: true } } },
     { $group: { _id: "$content", count: { $sum: 1 } } },
     { $sort: { count: -1 } },
     { $limit: 5 }
 ])

// All entries where the “content” field is less than 5 characters long;
db.tiktok_reviews.find(
    { $expr: { $lt: [{ $strLenCP: "$content" }, 5] } },
    {
        _id: 0,
        reviewId: 1, 
        content: 1, 
        length: { $strLenCP: "$content" } 
    }
)

// Average rating for each day (the result should be in timestamp type).
db.tiktok_reviews.aggregate([
    {
        $addFields: {
            date: { $toDate: "$at" } 
        }
    },
    {
        $group: {
            _id: { $dateTrunc: { date: "$date", unit: "day" } }, 
            averageScore: { $avg: "$score" } 
        }
    },
    {
        $sort: { _id: 1 } 
    }
])
