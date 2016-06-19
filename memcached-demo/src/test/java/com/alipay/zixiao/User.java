/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2016 ALL Rights Reserved
 */
package com.alipay.zixiao;

import java.io.Serializable;

/**
 * JAVA POLO
 * @author:zixiao.hzx
 * @version $Id:User.java,v0.1 2016-06-19 上午9:59 zixiao.hzx Exp $$
 */
public class User implements Serializable{

    private static final long serialVersionUID = -6091892250556951800L;

    private String name;

    private int age;

    public User(String name, int age) {
        this.name = name;
        this.age = age;
    }

    /**
     * Getter method for property name
     *
     * @return property value of name
     */
    public String getName() {
        return name;
    }

    /**
     * Setter method for property name.
     *
     * @param name value to be assigned to property name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Getter method for property age
     *
     * @return property value of age
     */
    public int getAge() {
        return age;
    }

    /**
     * Setter method for property age.
     *
     * @param age value to be assigned to property age
     */
    public void setAge(int age) {
        this.age = age;
    }


}
