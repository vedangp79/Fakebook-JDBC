package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }

    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Birthed, Month_of_Birth " + // select birth months and number of uses with that birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth IS NOT NULL " + // for which a birth month is available
                            "GROUP BY Month_of_Birth " + // group into buckets by birth month
                            "ORDER BY Birthed DESC, Month_of_Birth ASC"); // sort by users born in that month, descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) { // step through result rows/records one by one
                if (rst.isFirst()) { // if first record
                    mostMonth = rst.getInt(2); //   it is the month with the most
                }
                if (rst.isLast()) { // if last record
                    leastMonth = rst.getInt(2); //   it is the month with the least
                }
                total += rst.getInt(1); // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + mostMonth + " " + // born in the most popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + leastMonth + " " + // born in the least popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
            FirstNameInfo info = new FirstNameInfo();

            // Query for longest names (A)
            ResultSet rstLong = stmt.executeQuery(
                    "SELECT DISTINCT FIRST_NAME " +
                    "FROM " + UsersTable + " " +
                    "WHERE LENGTH(FIRST_NAME) = (SELECT MAX(LENGTH(FIRST_NAME)) FROM " + UsersTable + " ) " +
                    "ORDER BY FIRST_NAME");
            while (rstLong.next()) {
                info.addLongName(rstLong.getString(1));
            }

            // Query for shortest names (B)
            ResultSet rstShort = stmt.executeQuery(
                    "SELECT DISTINCT FIRST_NAME " +
                    "FROM " + UsersTable + " " +
                    "WHERE LENGTH(FIRST_NAME) = (SELECT MIN(LENGTH(FIRST_NAME)) FROM " + UsersTable + " ) " +
                    "ORDER BY FIRST_NAME");
            while (rstShort.next()) {
                info.addShortName(rstShort.getString(1));
            }
            
            // Query for most common name or names (C)
            // First, find the highest occurrence count
            ResultSet rstCount = stmt.executeQuery(
                    "SELECT FIRST_NAME, COUNT(*) AS COUNT " +
                    "FROM " + UsersTable + " " +
                    "GROUP BY FIRST_NAME " +
                    "ORDER BY COUNT DESC, FIRST_NAME");
            long maxCount = 0;
            if (rstCount.next()) {
                maxCount = rstCount.getLong(2);
                info.addCommonName(rstCount.getString(1));
                info.setCommonNameCount(maxCount);
                //if tie, remaining names and their count added below
                while (rstCount.next() && rstCount.getLong(2) == maxCount) {
                    info.addCommonName(rstCount.getString(1));
                }
            }

            rstLong.close();
            rstShort.close();
            rstCount.close();
            stmt.close(); 

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */

            ResultSet rst = stmt.executeQuery(
                    "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                       "FROM " + UsersTable + " U " +
                       "WHERE NOT EXISTS (" +
                       "SELECT * FROM " + FriendsTable + " F " +
                       "WHERE F.USER1_ID = U.USER_ID OR F.USER2_ID = U.USER_ID" +
                       ") " +
                       "ORDER BY U.USER_ID ASC");
            
            while (rst.next()) {
                long id = rst.getLong("USER_ID");
                String firstName = rst.getString("FIRST_NAME");
                String lastName = rst.getString("LAST_NAME");
                UserInfo user = new UserInfo(id, firstName, lastName);
                results.add(user);
            }     
            
            rst.close();
            stmt.close();

            return results;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return results;
        }
    }

    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT DISTINCT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                       "FROM " + UsersTable + " U " +
                       "JOIN " + CurrentCitiesTable + " CC ON U.USER_ID = CC.USER_ID " +
                       "JOIN " + HometownCitiesTable + " HC ON U.USER_ID = HC.USER_ID " +
                       "WHERE CC.CURRENT_CITY_ID != HC.HOMETOWN_CITY_ID " +
                       "ORDER BY U.USER_ID ASC");
            
            while (rst.next()) {
                long id = rst.getLong("USER_ID");
                String firstName = rst.getString("FIRST_NAME");
                String lastName = rst.getString("LAST_NAME");
                UserInfo user = new UserInfo(id, firstName, lastName);
                results.add(user);
            }

            rst.close();
            stmt.close();

            return results;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return results;
        }
    }

    @Override
   // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */
            String query = "SELECT P.PHOTO_ID, P.ALBUM_ID, P.PHOTO_LINK, A.ALBUM_NAME, COUNT(T.TAG_PHOTO_ID) AS TAG_COUNT " +
                       "FROM " + PhotosTable + " P " +
                       "JOIN " + TagsTable + " T ON P.PHOTO_ID = T.TAG_PHOTO_ID " +
                       "JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " +
                       "GROUP BY P.PHOTO_ID, P.ALBUM_ID, P.PHOTO_LINK, A.ALBUM_NAME " +
                       "ORDER BY TAG_COUNT DESC, P.PHOTO_ID ASC " +
                       "FETCH FIRST " + num + " ROWS ONLY";

            ResultSet rsPhotos = stmt.executeQuery(query);
            
            // Process each photo
            while (rsPhotos.next()) {
                long photoId = rsPhotos.getLong(1);
                long albumId = rsPhotos.getLong(2);
                String photoLink = rsPhotos.getString(3);
                String albumName = rsPhotos.getString(4);
                
                // Create PhotoInfo object
                PhotoInfo photoInfo = new PhotoInfo(photoId, albumId, photoLink, albumName);
                TaggedPhotoInfo taggedPhotoInfo = new TaggedPhotoInfo(photoInfo);
                
                // Query to get tagged users for the current photo
                String queryUsers = "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                                    "FROM " + TagsTable + " T " +
                                    "JOIN " + UsersTable + " U ON T.Tag_subject_ID = U.USER_ID " +
                                    "WHERE T.TAG_PHOTO_ID = " + photoId + " " +
                                    "ORDER BY U.USER_ID ASC";
                try (Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                    ResultSet rsUsers = stmt2.executeQuery(queryUsers);
                
                // Process each tagged user
                    while (rsUsers.next()) {
                        long userId = rsUsers.getLong(1);
                        String firstName = rsUsers.getString(2);
                        String lastName = rsUsers.getString(3);
                        
                        taggedPhotoInfo.addTaggedUser(new UserInfo(userId, firstName, lastName));
                    } 
                results.add(taggedPhotoInfo);
                rsUsers.close();
            }
        }
            rsPhotos.close();
            stmt.close();

            return results;
           
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return results;
        }
    }
 
  @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
            String findPairsQuery = 
            "SELECT U1.USER_ID AS USER1_ID, U1.FIRST_NAME AS USER1_FIRST, U1.LAST_NAME AS USER1_LAST, U1.year_of_birth AS USER1_YEAR, " +
            "U2.USER_ID AS USER2_ID, U2.FIRST_NAME AS USER2_FIRST, U2.LAST_NAME AS USER2_LAST, U2.YEAR_OF_BIRTH AS USER2_YEAR, " +
            "COUNT(DISTINCT T1.TAG_PHOTO_ID) AS COMMON_PHOTOS " +
            "FROM " + UsersTable + " U1 " +
            "JOIN " + UsersTable + " U2 ON U1.GENDER = U2.GENDER AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= " + yearDiff + " AND U1.USER_ID < U2.USER_ID " +
            "LEFT JOIN " + FriendsTable + " F ON (U1.USER_ID = F.USER1_ID AND U2.USER_ID = F.USER2_ID) OR (U1.USER_ID = F.USER2_ID AND U2.USER_ID = F.USER1_ID) " +
            "JOIN " + TagsTable + " T1 ON U1.USER_ID = T1.TAG_SUBJECT_ID " +
            "JOIN " + TagsTable + " T2 ON U2.USER_ID = T2.TAG_SUBJECT_ID AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID " +
            "WHERE F.USER1_ID IS NULL AND F.USER2_ID IS NULL " +
            "GROUP BY U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH " +
            "HAVING COUNT(DISTINCT T1.TAG_PHOTO_ID) > 0 " +
            "ORDER BY COMMON_PHOTOS DESC, U1.USER_ID ASC, U2.USER_ID ASC " +
            "FETCH FIRST " + num + " ROWS ONLY";

            // Execute the query
            ResultSet pairsRS = stmt.executeQuery(findPairsQuery);

            // Step 2: For each pair, find the photos in which they are both tagged
            

            // Prepare the statement for finding shared photos outside the loop
            // PreparedStatement pstmt = oracle.prepareStatement(findPhotosQuery);

            while (pairsRS.next()) {
                // Extract user info
                UserInfo user1 = new UserInfo(pairsRS.getLong(1), pairsRS.getString(2), pairsRS.getString(3));
                UserInfo user2 = new UserInfo(pairsRS.getLong(5), pairsRS.getString(6), pairsRS.getString(7));
                int user1Year = pairsRS.getInt(4);
                int user2Year = pairsRS.getInt(8);
    
                long user1Id = pairsRS.getLong(1);
                long user2Id = pairsRS.getLong(5);

                // Set parameters for the prepared statement
                String findPhotosQuery = 
                "SELECT DISTINCT P.PHOTO_ID, P.ALBUM_ID, P.PHOTO_LINK, A.ALBUM_NAME " +
                "FROM " + TagsTable + " T1 " +
                "JOIN " + TagsTable + " T2 ON T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND T1.TAG_SUBJECT_ID <> T2.TAG_SUBJECT_ID " +
                "JOIN " + PhotosTable + " P ON T1.TAG_PHOTO_ID = P.PHOTO_ID " +
                "JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " +
                "WHERE T1.TAG_SUBJECT_ID = " + Long.toString(user1Id) + " AND T2.TAG_SUBJECT_ID = " + Long.toString(user2Id) + " " +
                "ORDER BY P.PHOTO_ID ASC";

                // Execute the prepared statement
                MatchPair mp = new MatchPair(user1, user1Year, user2, user2Year);
                try (Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                    ResultSet photosRS = stmt2.executeQuery(findPhotosQuery);
                    while (photosRS.next()) {
                    PhotoInfo photo = new PhotoInfo(photosRS.getLong(1), photosRS.getLong(2), photosRS.getString(3), photosRS.getString(4));
                    mp.addSharedPhoto(photo);
                    }
                }
                results.add(mp);
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return results;
        }

        return results;
    }

    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */

           String query1 = "CREATE VIEW NotFriends AS " +
                    "SELECT U1.USER_ID AS user1, U1.FIRST_NAME AS first1, U1.LAST_NAME As last1, U2.USER_ID AS user2, U2.FIRST_NAME AS first2, U2.LAST_NAME AS last2 " +
                    "FROM " + UsersTable + " U1, " + UsersTable + " U2 " +  
                    "WHERE U1.user_ID < U2.user_ID AND (U1.user_id, U2.user_id) NOT IN " +
                        "(SELECT * FROM " + FriendsTable +  " ) ";
            String view = "CREATE VIEW PairGroup AS " +
                    "SELECT NF.user1 AS user1, NF.first1 AS first1, NF.last1 AS last1, NF.user2 AS user2, NF.first2 AS first2, NF.last2 AS last2, F1.USER1_ID AS common1, F1.USER2_ID AS common2 " +
                    "FROM NotFriends NF, " + FriendsTable + " F1, " + FriendsTable + " F2 " +
                    "WHERE (NF.user1 = F1.USER1_ID AND F1.USER2_ID = F2.USER1_ID AND NF.user2 = F2.USER2_ID) OR " +
                            "(NF.user1 = F1.USER2_ID AND F1.USER1_ID = F2.USER1_ID AND NF.user2 = F2.USER2_ID) OR " +
                            "(NF.user1 = F1.USER1_ID AND F1.USER2_ID = F2.USER2_ID AND NF.user2 = F2.USER1_ID) OR " +
                            "(NF.user1 = F1.USER2_ID AND F1.USER1_ID = F2.USER1_ID AND NF.user2 = F2.USER1_ID) " ;
            String query2 = 
                    "SELECT * FROM (SELECT PG.user1, PG.first1, PG.last1, PG.user2, PG.first2, PG.last2, COUNT(*) AS mutuals " +
                                    "FROM PairGroup PG " +
                                    "GROUP BY PG.user1, PG.first1, PG.last1, PG.user2, PG.first2, PG.last2 " +
                                    "ORDER BY mutuals DESC, PG.user1 ASC) " +
                    "WHERE ROWNUM <= " + num;
            String query4 = "DROP VIEW PairGroup " ;
            String query5 = "DROP VIEW NotFriends " ;            
            stmt.executeUpdate(query1);
            stmt.executeUpdate(view);
            ResultSet rst = stmt.executeQuery(query2);
            
            while(rst.next()) {
                long uid1 = rst.getLong(1);
                long uid2 = rst.getLong(4);
                UserInfo user1 = new UserInfo(rst.getLong(1),rst.getString(2),rst.getString(3));
                UserInfo user2 = new UserInfo(rst.getLong(4), rst.getString(5), rst.getString(6));
                UsersPair up = new UsersPair(user1, user2);
                try (Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                    String query3 = "SELECT DISTINCT U3.USER_ID, U3.FIRST_NAME, U3.LAST_NAME " +
                                    "FROM PairGroup PG " +
                                    "JOIN " + UsersTable + " U3 ON " + uid1 + " = PG.user1 AND " + uid2 + " = PG.user2 " +
                                    "WHERE U3.USER_ID != " + uid1 + " AND U3.USER_ID != " + uid2 + " AND " +
                                    "(U3.USER_ID = PG.common1 OR U3.USER_ID = PG.COMMON2)" + 
                                    "ORDER BY U3.USER_ID ASC";
                    ResultSet rst2 = stmt2.executeQuery(query3);
                    while(rst2.next()) {                                            
                        UserInfo friend = new UserInfo(rst2.getLong(1), rst2.getString(2), rst2.getString(3));
                        up.addSharedFriend(friend);
                    }
                results.add(up);
                rst2.close();
                stmt2.close();                     
                }      
            }
            stmt.executeUpdate(query4);
            stmt.executeUpdate(query5);
            rst.close();                       
            stmt.close();
            return results;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
           return results;
        }
    }

    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
            
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            String query = 
                "SELECT C.STATE_NAME, COUNT(*) AS num " +
                "FROM " + EventsTable + " E " +
                "JOIN " + CitiesTable + " C ON E.EVENT_CITY_ID = C.CITY_ID " +
                "GROUP BY C.STATE_NAME " +
                "ORDER BY num DESC " ;                

            ResultSet rs = stmt.executeQuery(query);
            EventStateInfo info = null;
            long maxEvents = 0;
            while (rs.next()) {
                if(info == null) {
                    maxEvents = rs.getLong(2);
                    info = new EventStateInfo(maxEvents);
                }
                if(rs.getLong(2) == maxEvents) {
                    info.addState(rs.getString(1));
                }
            }
            if(info == null) {
                info = new EventStateInfo(-1);
            }
            rs.close();
            stmt.close();
            return info;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }
    
    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            String dropview = "DROP VIEW AllFriends";
            String dropview1 = "DROP VIEW AF1";
            String dropview2 = "DROP VIEW AF2";
           String useridstring = Long.toString(userID);
           String query = "CREATE VIEW AF1 AS " +
                            "SELECT F1.user2_id as fid " +
                            "FROM " + FriendsTable + " F1 " +
                            "WHERE F1.user1_id = " + useridstring;
            String view1 = "CREATE VIEW AF2 AS " +
                            "SELECT F2.user1_id as fid " +
                            "FROM " + FriendsTable + " F2 " +
                            "WHERE F2.user1_id = " + useridstring;
            String view3 = "CREATE VIEW AllFriends AS " +
                            "SELECT * FROM AF1 UNION SELECT * FROM AF2";
            stmt.executeUpdate(query);
            stmt.executeUpdate(view1);
            stmt.executeUpdate(view3);
            String query2 = "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                            "FROM AllFriends AF " +
                            "JOIN " + UsersTable + " U ON AF.fid = U.USER_ID " +
                            "ORDER BY U.year_of_birth, U.month_of_birth, U.day_of_birth ASC";
            ResultSet rst = stmt.executeQuery(query2);
            Long oldID = 99999999L;
            Long youngID = 999999999L;
            String oldF = "";
            String oldL = "";
            String youngF = "";
            String youngL =  "";
            while(rst.next()) {
                if (rst.isFirst()) {
                    oldID = rst.getLong(1);
                    oldF = rst.getString(2);
                    oldL = rst.getString(3);
                }
                if (rst.isLast()) {
                    youngID = rst.getLong(1);
                    youngF = rst.getString(2);
                    youngL = rst.getString(3);
                }                
            }
            UserInfo old = new UserInfo(oldID, oldF, oldL);
            UserInfo young = new UserInfo(youngID, youngF, youngL);
            stmt.executeUpdate(dropview);
            stmt.executeUpdate(dropview1);
            stmt.executeUpdate(dropview2);
            rst.close();
            stmt.close();
            return new AgeInfo(old,young);
            
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
           String queryS = "CREATE VIEW MatchTable AS " +
           "SELECT U1.USER_ID AS user1, U2.USER_ID as user2 " +
            "FROM " + UsersTable + " U1, " + UsersTable + " U2 " +
            "WHERE U1.LAST_NAME = U2.LAST_NAME  AND (ABS(U1.year_of_birth - U2.year_of_birth) < 10) " +
            "INTERSECT " +
            "SELECT H1.USER_ID AS user1, H2.USER_ID AS user2 " +
            "FROM " + HometownCitiesTable + " H1, " + HometownCitiesTable + " H2 " +
            "WHERE H1.HOMETOWN_CITY_ID = H2.HOMETOWN_CITY_ID " +
            "INTERSECT " +
            "SELECT * FROM " + FriendsTable + "" ;
            stmt.executeUpdate(queryS);
            String queryR = "SELECT U1.USER_ID AS u1id, U1.First_Name as u1fname, U1.Last_Name as u1lname, U2.USER_ID AS u2id, U2.First_Name as u2fname, U2.Last_Name as u2lname " +
                            "FROM " + UsersTable + " U1, " + UsersTable + " U2, MatchTable Ms " +
                            "WHERE Ms.user1 = U1.USER_ID AND Ms.user2 = U2.USER_ID" ;
            ResultSet rs2 = stmt.executeQuery(queryR);
            while(rs2.next()) {
                UserInfo u1 = new UserInfo(rs2.getLong(1), rs2.getString(2), rs2.getString(3));
                UserInfo u2 = new UserInfo(rs2.getLong(4), rs2.getString(5), rs2.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);                
            }
            String query3 = "DROP VIEW MatchTable" ;
            stmt.executeUpdate(query3);
            rs2.close();
            stmt.close();


        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return results;
        }

        return results;
    }

    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
