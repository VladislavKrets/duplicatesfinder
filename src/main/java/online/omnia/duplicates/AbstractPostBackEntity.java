package online.omnia.duplicates;

import javax.persistence.*;
import java.sql.Date;
import java.sql.Time;

/**
 * Created by lollipop on 11.07.2017.
 */
@MappedSuperclass
public abstract class AbstractPostBackEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private int id;
    @Column(name = "date")
    private Date date;
    @Column(name = "time")
    private Time time;
    @Column(name = "prefix", length = 50)
    private String prefix;
    @Column(name = "clickid", length = 100)
    private String clickId;
    @Column(name = "sum")
    private double sum;
    @Column(name = "currency", length = 3)
    private String currency;
    @Column(name = "goal", length = 50)
    private String goal;
    @Column(name = "afid")
    private int afid;
    @Column(name = "status", length = 50)
    private String status;
    @Column(name = "advname", length = 100)
    private String advName;
    @Column(name = "offername", length = 100)
    private String offerName;
    @Column(name = "transactionid", length = 50)
    private String transactionId;
    @Column(name = "idfa", length = 50)
    private String IDFA;
    @Column(name = "gaid", length = 50)
    private String GaId;
    @Column(name = "t1", length = 100)
    private String t1;
    @Column(name = "t2", length = 100)
    private String t2;
    @Column(name = "t3", length = 100)
    private String t3;
    @Column(name = "t4", length = 100)
    private String t4;
    @Column(name = "t5", length = 100)
    private String t5;
    @Column(name = "t6", length = 100)
    private String t6;
    @Column(name = "t7", length = 100)
    private String t7;
    @Column(name = "t8", length = 100)
    private String t8;
    @Column(name = "t9", length = 100)
    private String t9;
    @Column(name = "t10", length = 100)
    private String t10;
    @Column(name = "secretkey", length = 50)
    private String secretKey;
    @Column(name = "ipaddress", length = 20)
    private String ipAddress;
    @Column(name = "fullurl", length = 500)
    private String fullURL;
    @Column(name = "postbacksend")
    private int postbackSend;
    @Column(name = "offerid")
    private String offerId;
    @Column(name = "duplicate")
    private String duplicate;
    @Transient
    private String actionId;
    public AbstractPostBackEntity() {
    }

    public AbstractPostBackEntity(int id, Date date, Time time, String prefix, String clickId, double sum, String currency,
                                  String goal, int afid, String status, String advName, String offerName, String transactionId,
                                  String IDFA, String gaId, String t1, String t2, String t3, String t4, String t5,
                                  String t6, String t7, String t8, String t9, String t10, String secretKey,
                                  String ipAddress, String fullURL, int postbackSend, String offerId, String duplicate) {
        this.id = id;
        this.date = date;
        this.time = time;
        this.prefix = prefix;
        this.clickId = clickId;
        this.sum = sum;
        this.currency = currency;
        this.goal = goal;
        this.afid = afid;
        this.status = status;
        this.advName = advName;
        this.offerName = offerName;
        this.transactionId = transactionId;
        this.IDFA = IDFA;
        GaId = gaId;
        this.t1 = t1;
        this.t2 = t2;
        this.t3 = t3;
        this.t4 = t4;
        this.t5 = t5;
        this.t6 = t6;
        this.t7 = t7;
        this.t8 = t8;
        this.t9 = t9;
        this.t10 = t10;
        this.secretKey = secretKey;
        this.ipAddress = ipAddress;
        this.fullURL = fullURL;
        this.postbackSend = postbackSend;
        this.offerId = offerId;
        this.duplicate = duplicate;
    }

    public int getId() {
        return id;
    }

    public Date getDate() {
        return date;
    }

    public Time getTime() {
        return time;
    }

    public String getClickId() {
        return clickId;
    }

    public double getSum() {
        return sum;
    }

    public String getCurrency() {
        return currency;
    }

    public String getGoal() {
        return goal;
    }

    public String getStatus() {
        return status;
    }

    public String getAdvName() {
        return advName;
    }

    public String getOfferName() {
        return offerName;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getIDFA() {
        return IDFA;
    }

    public String getGaId() {
        return GaId;
    }

    public String getT1() {
        return t1;
    }

    public String getT2() {
        return t2;
    }

    public String getT3() {
        return t3;
    }

    public String getT4() {
        return t4;
    }

    public String getT5() {
        return t5;
    }

    public String getT6() {
        return t6;
    }

    public String getT7() {
        return t7;
    }

    public String getT8() {
        return t8;
    }

    public String getT9() {
        return t9;
    }

    public String getT10() {
        return t10;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getFullURL() {
        return fullURL;
    }

    public int getPostbackSend() {
        return postbackSend;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public void setTime(Time time) {
        this.time = time;
    }

    public void setClickId(String clickId) {
        this.clickId = clickId;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public void setGoal(String goal) {
        this.goal = goal;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setAdvName(String advName) {
        this.advName = advName;
    }

    public void setOfferName(String offerName) {
        this.offerName = offerName;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public void setIDFA(String IDFA) {
        this.IDFA = IDFA;
    }

    public void setGaId(String gaId) {
        GaId = gaId;
    }

    public void setT1(String t1) {
        this.t1 = t1;
    }

    public void setT2(String t2) {
        this.t2 = t2;
    }

    public void setT3(String t3) {
        this.t3 = t3;
    }

    public void setT4(String t4) {
        this.t4 = t4;
    }

    public void setT5(String t5) {
        this.t5 = t5;
    }

    public void setT6(String t6) {
        this.t6 = t6;
    }

    public void setT7(String t7) {
        this.t7 = t7;
    }

    public void setT8(String t8) {
        this.t8 = t8;
    }

    public void setT9(String t9) {
        this.t9 = t9;
    }

    public void setT10(String t10) {
        this.t10 = t10;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public void setFullURL(String fullURL) {
        this.fullURL = fullURL;
    }

    public void setPostbackSend(int postbackSend) {
        this.postbackSend = postbackSend;
    }

    public int getAfid() {
        return afid;
    }

    public void setAfid(int afid) {
        this.afid = afid;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getOfferId() {
        return offerId;
    }

    public void setOfferId(String offerId) {
        this.offerId = offerId;
    }

    @Override
    public String toString() {
        return "PostBackEntity{" +
                "id=" + id +
                ", date=" + date +
                ", time=" + time +
                ", prefix=" + prefix +
                ", clickId='" + clickId + '\'' +
                ", sum=" + sum +
                ", currency='" + currency + '\'' +
                ", goal=" + goal +
                ", afid=" + afid +
                ", status=" + status +
                ", advName='" + advName + '\'' +
                ", offerName='" + offerName + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", IDFA='" + IDFA + '\'' +
                ", GaId='" + GaId + '\'' +
                ", t1='" + t1 + '\'' +
                ", t2='" + t2 + '\'' +
                ", t3='" + t3 + '\'' +
                ", t4='" + t4 + '\'' +
                ", t5='" + t5 + '\'' +
                ", t6='" + t6 + '\'' +
                ", t7='" + t7 + '\'' +
                ", t8='" + t8 + '\'' +
                ", t9='" + t9 + '\'' +
                ", t10='" + t10 + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", fullURL='" + fullURL + '\'' +
                ", postbackSend=" + postbackSend +
                ", offerId='" + offerId + '\'' +
                '}';
    }

    public String getActionId() {
        return actionId;
    }

    public void setActionId(String actionId) {
        this.actionId = actionId;
    }

    public String getDuplicate() {
        return duplicate;
    }

    public void setDuplicate(String duplicate) {
        this.duplicate = duplicate;
    }
}
