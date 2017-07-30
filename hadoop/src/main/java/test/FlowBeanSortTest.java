package test;

import mapreduce.flowcount.FlowBean;

import java.util.ArrayList;

public class FlowBeanSortTest {
    public static void main(String[] args){
        ArrayList<FlowBean> arrList = new ArrayList<FlowBean>();
        FlowBean bean = new FlowBean();
        bean.set(2,3);
        arrList.add(bean);

        bean.set(1,5);
        arrList.add(bean);

        bean.set(6,5);
        arrList.add(bean);

        bean.set(7,5);
        arrList.add(bean);

//        for (FlowBean bean: arrList.){
//
//        }

    }
}
