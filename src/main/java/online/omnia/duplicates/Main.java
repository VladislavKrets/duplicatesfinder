package online.omnia.duplicates;

import java.util.List;

/**
 * Created by lollipop on 30.08.2017.
 */
public class Main {
    public static void main(String[] args) {
        List<PostBackEntity> postBackEntities;
        for (int i = 0; true; i+=100) {
            postBackEntities = MySQLDaoImpl.getInstance().getPostbackEntities(i, 99);
            if (postBackEntities.isEmpty()) break;
            for (PostBackEntity postBackEntity : postBackEntities) {
                if (MySQLDaoImpl.getInstance().isClickIdPartitial(postBackEntity)) {
                    MySQLDaoImpl.getInstance().updatePartitialPostbackEntities(postBackEntity);
                }
                if (MySQLDaoImpl.getInstance().isFullUrlPostback(postBackEntity)) {
                    System.out.println("FULL");
                    MySQLDaoImpl.getInstance().updateFullPostbackEntities(postBackEntity);
                }
            }
        }
        postBackEntities = null;
        MySQLDaoImpl.getSessionFactory().close();
    }
}
