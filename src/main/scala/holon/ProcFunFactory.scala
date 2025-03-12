package holon

trait ProcFunFactory {
    def create(partition: Int): ProcFun
}
