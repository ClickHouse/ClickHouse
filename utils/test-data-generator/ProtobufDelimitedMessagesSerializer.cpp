// Reference file generator for the test dbms/tests/queries/0_stateless/00825_protobuf_format_output.sh

#include <iostream>
#include <google/protobuf/util/delimited_message_util.h>
#include "00825_protobuf_format.pb.h"
#include "00825_protobuf_format_syntax2.pb.h"


int main(int, char **)
{
    std::ostream* out = &std::cout;

    {
        Person person;
        person.set_uuid("a7522158-3d41-4b77-ad69-6c598ee55c49");
        person.set_name("Ivan");
        person.set_surname("Petrov");
        person.set_gender(Gender::male);
        person.set_birthdate(4015); // 1980-12-29
        person.set_photo("png");
        person.set_phonenumber(std::string("+74951234567\0", 13)); // Converted from FixedString(13)
        person.set_isonline(true);
        person.set_visittime(1546703100); // 2019-01-05 18:45:00
        person.set_age(38);
        person.set_zodiacsign(ZodiacSign::capricorn);
        person.add_songs("Yesterday");
        person.add_songs("Flowers");
        person.add_color(255);
        person.add_color(0);
        person.add_color(0);
        person.set_hometown("Moscow");
        person.add_location(55.753215);
        person.add_location(37.622504);
        person.set_pi(3.14);
        person.set_lotterywin(214.10);
        person.set_someratio(0.1);
        person.set_temperature(5.8);
        person.set_randombignumber(17060000000);
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    {
        Person person;
        person.set_uuid("c694ad8a-f714-4ea3-907d-fd54fb25d9b5");
        person.set_name("Natalia");
        person.set_surname("Sokolova");
        person.set_gender(Gender::female);
        person.set_birthdate(8102); // 1992-03-08
        person.set_photo("jpg");
        person.set_isonline(false);
        person.set_age(26);
        person.set_zodiacsign(ZodiacSign::pisces);
        person.add_color(100);
        person.add_color(200);
        person.add_color(50);
        person.set_hometown("Plymouth");
        person.add_location(50.403724);
        person.add_location(-4.142123);
        person.set_pi(3.14159);
        person.set_someratio(0.007);
        person.set_temperature(5.4);
        person.set_randombignumber(-20000000000000);
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    {
        Person person;
        person.set_uuid("a7da1aa6-f425-4789-8947-b034786ed374");
        person.set_name("Vasily");
        person.set_surname("Sidorov");
        person.set_gender(Gender::male);
        person.set_birthdate(9339); // 1995-07-28
        person.set_photo("bmp");
        person.set_phonenumber("+442012345678");
        person.set_isonline(true);
        person.set_visittime(1546117200); // 2018-12-30 00:00:00
        person.set_age(23);
        person.set_zodiacsign(ZodiacSign::leo);
        person.add_songs("Sunny");
        person.add_color(250);
        person.add_color(244);
        person.add_color(10);
        person.set_hometown("Murmansk");
        person.add_location(68.970682);
        person.add_location(33.074981);
        person.set_pi(3.14159265358979);
        person.set_lotterywin(100000000000);
        person.set_someratio(800);
        person.set_temperature(-3.2);
        person.set_randombignumber(154400000);
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    *out << "ALTERNATIVE->" << std::endl;

    {
        AltPerson person;
        person.add_location(55);
        person.add_location(37);
        person.set_pi(3.14);
        person.set_uuid("a7522158-3d41-4b77-ad69-6c598ee55c49");
        person.set_name("Ivan");
        person.set_gender(AltPerson::male);
        person.set_zodiacsign(1222); // capricorn
        person.set_birthdate(4015); // 1980-12-29
        person.set_age("38");
        person.set_isonline(OnlineStatus::online);
        person.set_someratio(0.100000001490116119384765625); // 0.1 converted from float to double
        person.set_visittime(1546703100); // 2019-01-05 18:45:00
        person.set_randombignumber(17060000000);
        person.add_color(255);
        person.add_color(0);
        person.add_color(0);
        person.set_lotterywin(214);
        person.set_surname("Petrov");
        person.set_phonenumber(+74951234567);
        person.set_temperature(5);
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    {
        AltPerson person;
        person.add_location(50);
        person.add_location(-4);
        person.set_pi(3.14159);
        person.set_uuid("c694ad8a-f714-4ea3-907d-fd54fb25d9b5");
        person.set_name("Natalia");
        person.set_gender(AltPerson::female);
        person.set_zodiacsign(219); // pisces
        person.set_birthdate(8102); // 1992-03-08
        person.set_age("26");
        person.set_isonline(OnlineStatus::offline);
        person.set_someratio(0.007000000216066837310791015625); // 0.007 converted from float to double
        person.set_randombignumber(-20000000000000);
        person.add_color(100);
        person.add_color(200);
        person.add_color(50);
        person.set_surname("Sokolova");
        person.set_temperature(5);
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    {
        AltPerson person;
        person.add_location(68);
        person.add_location(33);
        person.set_pi(3.1415926535897);
        person.set_uuid("a7da1aa6-f425-4789-8947-b034786ed374");
        person.set_name("Vasily");
        person.set_gender(AltPerson::male);
        person.set_zodiacsign(723); // leo
        person.set_birthdate(9339); // 1995-07-28
        person.set_age("23");
        person.set_isonline(OnlineStatus::online);
        person.set_someratio(800);
        person.set_visittime(1546117200); // 2018-12-30 00:00:00
        person.set_randombignumber(154400000);
        person.add_color(250);
        person.add_color(244);
        person.add_color(10);
        person.set_lotterywin(100000000000);
        person.set_surname("Sidorov");
        person.set_phonenumber(+442012345678);
        person.set_temperature(-3);
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    *out << "STRINGS->" << std::endl;

    {
        StrPerson person;
        person.set_uuid("a7522158-3d41-4b77-ad69-6c598ee55c49");
        person.set_name("Ivan");
        person.set_surname("Petrov");
        person.set_gender("male");
        person.set_birthdate("1980-12-29");
        person.set_phonenumber(std::string("+74951234567\0", 13)); // Converted from FixedString(13)
        person.set_isonline("1");
        person.set_visittime("2019-01-05 18:45:00");
        person.set_age("38");
        person.set_zodiacsign("capricorn");
        person.add_songs("Yesterday");
        person.add_songs("Flowers");
        person.add_color("255");
        person.add_color("0");
        person.add_color("0");
        person.set_hometown("Moscow");
        person.add_location("55.753215");
        person.add_location("37.622504");
        person.set_pi("3.14");
        person.set_lotterywin("214.10");
        person.set_someratio("0.1");
        person.set_temperature("5.8");
        person.set_randombignumber("17060000000");
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    {
        StrPerson person;
        person.set_uuid("c694ad8a-f714-4ea3-907d-fd54fb25d9b5");
        person.set_name("Natalia");
        person.set_surname("Sokolova");
        person.set_gender("female");
        person.set_birthdate("1992-03-08");
        person.set_isonline("0");
        person.set_age("26");
        person.set_zodiacsign("pisces");
        person.add_color("100");
        person.add_color("200");
        person.add_color("50");
        person.set_hometown("Plymouth");
        person.add_location("50.403724");
        person.add_location("-4.142123");
        person.set_pi("3.14159");
        person.set_someratio("0.007");
        person.set_temperature("5.4");
        person.set_randombignumber("-20000000000000");
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    {
        StrPerson person;
        person.set_uuid("a7da1aa6-f425-4789-8947-b034786ed374");
        person.set_name("Vasily");
        person.set_surname("Sidorov");
        person.set_gender("male");
        person.set_birthdate("1995-07-28");
        person.set_phonenumber("+442012345678");
        person.set_isonline("1");
        person.set_visittime("2018-12-30 00:00:00");
        person.set_age("23");
        person.set_zodiacsign("leo");
        person.add_songs("Sunny");
        person.add_color("250");
        person.add_color("244");
        person.add_color("10");
        person.set_hometown("Murmansk");
        person.add_location("68.970682");
        person.add_location("33.074981");
        person.set_pi("3.14159265358979");
        person.set_lotterywin("100000000000.00");
        person.set_someratio("800");
        person.set_temperature("-3.2");
        person.set_randombignumber("154400000");
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    *out << "SYNTAX2->" << std::endl;

    {
        Syntax2Person person;
        person.set_uuid("a7522158-3d41-4b77-ad69-6c598ee55c49");
        person.set_name("Ivan");
        person.set_surname("Petrov");
        person.set_gender(Syntax2Person::male);
        person.set_birthdate(4015); // 1980-12-29
        person.set_photo("png");
        person.set_phonenumber(std::string("+74951234567\0", 13)); // Converted from FixedString(13)
        person.set_isonline(true);
        person.set_visittime(1546703100); // 2019-01-05 18:45:00
        person.set_age(38);
        person.set_zodiacsign(Syntax2Person::capricorn);
        person.add_songs("Yesterday");
        person.add_songs("Flowers");
        person.add_color(255);
        person.add_color(0);
        person.add_color(0);
        person.set_hometown("Moscow");
        person.add_location(55.753215);
        person.add_location(37.622504);
        person.set_pi(3.14);
        person.set_lotterywin(214.10);
        person.set_someratio(0.1);
        person.set_temperature(5.8);
        person.set_randombignumber(17060000000);
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    {
        Syntax2Person person;
        person.set_uuid("c694ad8a-f714-4ea3-907d-fd54fb25d9b5");
        person.set_name("Natalia");
        person.set_surname("Sokolova");
        person.set_gender(Syntax2Person::female);
        person.set_birthdate(8102); // 1992-03-08
        person.set_photo("jpg");
        person.set_isonline(false);
        person.set_age(26);
        person.set_zodiacsign(Syntax2Person::pisces);
        person.add_color(100);
        person.add_color(200);
        person.add_color(50);
        person.set_hometown("Plymouth");
        person.add_location(50.403724);
        person.add_location(-4.142123);
        person.set_pi(3.14159);
        person.set_someratio(0.007);
        person.set_temperature(5.4);
        person.set_randombignumber(-20000000000000);
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    {
        Syntax2Person person;
        person.set_uuid("a7da1aa6-f425-4789-8947-b034786ed374");
        person.set_name("Vasily");
        person.set_surname("Sidorov");
        person.set_gender(Syntax2Person::male);
        person.set_birthdate(9339); // 1995-07-28
        person.set_photo("bmp");
        person.set_phonenumber("+442012345678");
        person.set_isonline(true);
        person.set_visittime(1546117200); // 2018-12-30 00:00:00
        person.set_age(23);
        person.set_zodiacsign(Syntax2Person::leo);
        person.add_songs("Sunny");
        person.add_color(250);
        person.add_color(244);
        person.add_color(10);
        person.set_hometown("Murmansk");
        person.add_location(68.970682);
        person.add_location(33.074981);
        person.set_pi(3.14159265358979);
        person.set_lotterywin(100000000000);
        person.set_someratio(800);
        person.set_temperature(-3.2);
        person.set_randombignumber(154400000);
        google::protobuf::util::SerializeDelimitedToOstream(person, out);
    }

    return 0;
}
