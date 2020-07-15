public class CountStart{
    public static void main(String[] args) {
        CountMonitordata countMonitordata = new CountMonitordata() {
            @Override
            public void invoke() {
                super.invoke();
            }
        };
        countMonitordata.invoke();
    }
}
