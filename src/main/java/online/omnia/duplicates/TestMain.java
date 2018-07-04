package online.omnia.duplicates;

import org.hibernate.Session;
import org.hibernate.SessionFactory;

/**
 * Created by lollipop on 30.08.2017.
 */
public class TestMain {
    public static void main(String[] args) {
        MySQLDaoImpl.getInstance();
        SessionFactory factory = MySQLDaoImpl.getSessionFactory();
        Session session = factory.openSession();
        session.beginTransaction();
        session.createQuery("update PostBackEntity set duplicate='original'").executeUpdate();
        session.getTransaction().commit();
        session.close();
    }
}
