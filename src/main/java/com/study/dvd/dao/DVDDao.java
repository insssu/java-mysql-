package com.study.dvd.dao;


import com.mysql.cj.protocol.Resultset;
import com.study.dvd.db.DBConnectionMgr;
import com.study.dvd.entity.DVD;
import com.study.dvd.entity.Producer;
import com.study.dvd.entity.Publisher;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class DVDDao {
    public static int addDvd(DVD dvd) {
        DBConnectionMgr pool = DBConnectionMgr.getInstance();

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        int successCount = 0;

        try {
            connection = pool.getConnection();       // 데이터베이스 연결
            String sql = "insert into dvd_tb values(0, ?, ?, ?, ?, ?, now())";      // 괄호 안 '?' 는 문법임. 어떤게 들어올지 모른다.
            preparedStatement = connection.prepareStatement(sql);                   // preparedStatement : 실행 할 쿼리를 닫기도 하고, '?'가 오면 데이터를 처리하기도 하는 가장 중요한 친구
            preparedStatement.setString(1, dvd.getRegistrationNumber());            // 쿼리 받고, sql 받고, ? 채워주고, 실행시키고.
            preparedStatement.setString(2, dvd.getTitle());
            preparedStatement.setInt(3, dvd.getProducer().getProducerId());
            preparedStatement.setInt(4, dvd.getPublisher().getPublisherId());
            preparedStatement.setInt(5, dvd.getPublicationYear());
            successCount = preparedStatement.executeUpdate();

        } catch (Exception e) {

        } finally {
            pool.freeConnection(connection, preparedStatement);
        }

        return successCount;
    }

    public static int addProducer(Producer producer) {
        DBConnectionMgr pool = DBConnectionMgr.getInstance();

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet generatedKeys = null;
        int successCount = 0;

        try {
            connection = pool.getConnection();       // 데이터베이스 연결
            String sql = "insert into producer_tb values(0, ?)";      // 괄호 안 '?' 는 문법임. 어떤게 들어올지 모른다. ? 자리에 test 제작사 < 값이 들어간 것. 매개변수를 '?'로 받은 것.
            preparedStatement = connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);           // 미완성된 쿼리 실행시킬 준비. generated_keys 를 가져오는 것은 auto increment 의 값을 필요로 할 때, 방금 생성한 값을 가져와라
            preparedStatement.setString(1, producer.getProducerName());     // ? 자리에 데이터 채우기                  // auto increment 의 값은 왜 필요한가? producer_id와 publisher_id는 값을 넣자마자 가져와서 써야할 때
            successCount = preparedStatement.executeUpdate();       // 쿼리 실행
            generatedKeys = preparedStatement.getGeneratedKeys();
            generatedKeys.next();
            producer.setProducerId(generatedKeys.getInt(1));    // 방금 만든 producer 의 id값.
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pool.freeConnection(connection, preparedStatement, generatedKeys);
        }

        return successCount;
    }

    public static int addPublisher(Publisher publisher) {
        DBConnectionMgr pool = DBConnectionMgr.getInstance();

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet generatedKeys = null;
        int successCount = 0;

        try {
            connection = pool.getConnection();       // 데이터베이스 연결
            String sql = "insert into publisher_tb values(0, ?)";      // 괄호 안 '?' 는 문법임. 어떤게 들어올지 모른다.
            preparedStatement = connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);           // 미완성된 쿼리 실행시킬 준비
            preparedStatement.setString(1, publisher.getPublisherName());     // ? 자리에 데이터 채우기
            successCount = preparedStatement.executeUpdate();       // 쿼리 실행
            generatedKeys = preparedStatement.getGeneratedKeys();
            generatedKeys.next();
            publisher.setPublisherId(generatedKeys.getInt(1));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pool.freeConnection(connection, preparedStatement, generatedKeys);
        }

        return successCount;
    }

    public static List<DVD> findAll(int count) {
        DBConnectionMgr pool = DBConnectionMgr.getInstance();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<DVD> dvdList = new ArrayList<>();

        try {
            connection = pool.getConnection();
            String sql = "select * from dvd_view limit 0, ?";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setInt(1, count);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                Producer producer = Producer.builder()
                        .producerId(resultSet.getInt(4))
                        .producerName(resultSet.getString(5))
                        .build();
                Publisher publisher = Publisher.builder()
                        .publisherId(resultSet.getInt(6))
                        .publisherName(resultSet.getString(7))
                        .build();
                DVD dvd = DVD.builder()
                        .dvdId(resultSet.getInt(1))
                        .registrationNumber(resultSet.getString(2))
                        .title(resultSet.getString(3))                      // resultSet : 쿼리 실행 결과를 그대로 가져온다.
                        .producerId(producer.getProducerId())
                        .producer(producer)
                        .publisherId(publisher.getPublisherId())
                        .publisher(publisher)
                        .publicationYear(resultSet.getInt(8))
                        .databaseDate(resultSet.getDate(9).toLocalDate())
                        .build();
                dvdList.add(dvd);
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pool.freeConnection(connection);
        }
        return dvdList;
    }



}
