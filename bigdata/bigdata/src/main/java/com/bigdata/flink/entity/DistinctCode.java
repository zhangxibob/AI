package com.bigdata.flink.entity;

/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class DistinctCode {
    //id编号
    private int id;
    //省份
    private String province;
    //省份编号
    private String provinceCode;
    //城市
    private String city;
    //城市编号
    private String cityCode;
    //gdp
    private int gdp;

    public DistinctCode(int id, String province, String provinceCode, String city, String cityCode, int gdp) {
        this.id = id;
        this.province = province;
        this.provinceCode = provinceCode;
        this.city = city;
        this.cityCode = cityCode;
        this.gdp = gdp;
    }

    @Override
    public String toString() {
        return "DistinctCode{" +
                "id=" + id +
                ", province='" + province + '\'' +
                ", provinceCode='" + provinceCode + '\'' +
                ", city='" + city + '\'' +
                ", cityCode='" + cityCode + '\'' +
                ", gdp=" + gdp +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getProvinceCode() {
        return provinceCode;
    }

    public void setProvinceCode(String provinceCode) {
        this.provinceCode = provinceCode;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCityCode() {
        return cityCode;
    }

    public void setCityCode(String cityCode) {
        this.cityCode = cityCode;
    }

    public int getGdp() {
        return gdp;
    }

    public void setGdp(int gdp) {
        this.gdp = gdp;
    }
}
