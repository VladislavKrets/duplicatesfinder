package online.omnia.duplicates;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import javax.persistence.PersistenceException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lollipop on 30.08.2017.
 */
public class MySQLDaoImpl {
    private static Configuration configuration;
    private static SessionFactory sessionFactory;
    private static MySQLDaoImpl instance;

    static {
        configuration = new Configuration()
                .addAnnotatedClass(PostBackEntity.class)
                .addAnnotatedClass(ErrorPostBackEntity.class)
                .configure("/hibernate.cfg.xml");
        Map<String, String> properties = FileWorkingUtils.iniFileReader();
        configuration.setProperty("hibernate.connection.password", properties.get("password"));
        configuration.setProperty("hibernate.connection.username", properties.get("username"));
        configuration.setProperty("hibernate.connection.url", properties.get("url"));
        while (true) {
            try {
                sessionFactory = configuration.buildSessionFactory();
                break;
            } catch (PersistenceException e) {
                try {
                    System.out.println("Can't connect to db");
                    System.out.println("Waiting for 30 seconds");
                    Thread.sleep(30000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }

    }
    public List<PostBackEntity> getPostbackEntities(int from, int to) {
        Session session = sessionFactory.openSession();
        List<PostBackEntity> postBackEntities = session.createQuery("from PostBackEntity", PostBackEntity.class)
                .setFirstResult(from).setMaxResults(to).getResultList();
        session.close();
        return postBackEntities;
    }

    public boolean isFullClickTransactionPostback(PostBackEntity postBackEntity) {
        Session session = sessionFactory.openSession();
        List<PostBackEntity> postBackEntities;
        if (postBackEntity.getStatus() == null) {
            postBackEntities = session.createQuery("from PostBackEntity where clickid=:clickId and transactionid=:transactionId", PostBackEntity.class)
                    .setParameter("clickId", postBackEntity.getClickId())
                    .setParameter("transactionId", postBackEntity.getTransactionId())
                    .getResultList();
        }
        else {
            postBackEntities = session.createQuery("from PostBackEntity where clickid=:clickId and transactionid=:transactionId and status=:status", PostBackEntity.class)
                    .setParameter("clickId", postBackEntity.getClickId())
                    .setParameter("transactionId", postBackEntity.getTransactionId())
                    .setParameter("status", postBackEntity.getStatus())
                    .getResultList();
        }
        session.close();
        boolean isFull = postBackEntities.size() > 1;
        postBackEntities = null;
        return isFull;
    }

    public boolean isClickIdPartitial(PostBackEntity postBackEntity) {
        Session session = sessionFactory.openSession();
        List<PostBackEntity> postBackEntities = session.createQuery("from PostBackEntity where clickid=:clickId", PostBackEntity.class)
                .setParameter("clickId", postBackEntity.getClickId())
                .getResultList();
        session.close();
        boolean isPartitial = postBackEntities.size() > 1;
        postBackEntities = null;
        return isPartitial;
    }

    public boolean isFullUrlPostback(PostBackEntity postBackEntity) {
        Session session = sessionFactory.openSession();
        List<PostBackEntity> postBackEntities = session.createQuery("from PostBackEntity where fullurl=:fullUrl", PostBackEntity.class)
                .setParameter("fullUrl", postBackEntity.getFullURL()).getResultList();
        session.close();
        boolean isFull = postBackEntities.size() > 1;
        postBackEntities = null;
        return isFull;
    }
    public void updateFullPostbackEntities(PostBackEntity postBackEntity) {
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        session.createQuery("update PostBackEntity set duplicate=:duplicate where fullurl=:fullUrl")
                .setParameter("duplicate", "FULL")
                .setParameter("fullUrl", postBackEntity.getFullURL())
                .executeUpdate();
        session.getTransaction().commit();
        session.close();
    }

    public void updatePartitialPostbackEntities(PostBackEntity postBackEntity) {
        Session session = sessionFactory.openSession();
        session.beginTransaction();
            session.createQuery("update PostBackEntity set duplicate=:duplicate where clickid=:clickId and duplicate!=:fullDuplicate")
                    .setParameter("duplicate", "PARTIAL")
                    .setParameter("clickId", postBackEntity.getClickId())
                    .setParameter("fullDuplicate", "FULL")
                    .executeUpdate();
        session.getTransaction().commit();
        session.close();
    }
    public static SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public static MySQLDaoImpl getInstance() {
        if (instance == null) instance = new MySQLDaoImpl();
        return instance;
    }
}
