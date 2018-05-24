

import org.neo4j.graphdb.RelationshipType;

/**
 * Created by Administrator on 2017/5/31 0031.
 */
public enum RelationshipTypes implements RelationshipType {
    //定义边关系类别
    IDCARD,BANKCARD,RETURNBANKCARD ,MYPHONE, EMAIL, CONTACT, EMERGENCY, DEVICE,COMPANYPHONE;
}
