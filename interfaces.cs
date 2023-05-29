public interface IPullData{

    Task<IEnumerable<object>> Pull(int batchNumber);    


}

public interface IProcessData {

    Task Process();

}

public interface IPushData{

    Task Push(byte[] content);

}