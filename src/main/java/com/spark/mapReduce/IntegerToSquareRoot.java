package com.spark.mapReduce;

import java.io.Serializable;

/**
 * @author Chetan Raj
 * @implNote
 * @since : 16/05/19
 */
public class IntegerToSquareRoot implements Serializable{
    private Integer num;
    private Double sqrtRoot;

    public IntegerToSquareRoot(Integer num) {
        this.num = num;
        this.sqrtRoot = Math.sqrt(num);
    }

    @Override
    public String toString() {
        return "IntegerToSquareRoot{" +
                "num=" + num +
                ", sqrtRoot=" + sqrtRoot +
                '}';
    }
}
