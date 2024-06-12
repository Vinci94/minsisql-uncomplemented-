#include <iostream>

struct D;
struct B {
  B() 
  {
    std::cout << "B::B()" << std::endl;
  }
  virtual operator D() = 0;
};
struct D : B {
  // D(D& other) 
  // {
  //   std::cout << "D::D(D&)" << std::endl;
  // }
  D() 
  {
    std::cout << "D::D()" << std::endl;
  }
  operator D() override 
  {
    std::cout << "D::operator D()" << std::endl;
    return D();
  }
};

int main() {
  D obj;
  D obj2 = obj;  // 不调用 D::operator D()
  B &br = obj;
  D obj3 = br;  // 通过虚派发调用 D::operator D()
}
